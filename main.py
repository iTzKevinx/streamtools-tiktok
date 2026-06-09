import asyncio
import json
import logging
import re
import time
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("streamtools")

app = FastAPI()

@app.get("/")
def root():
    return {"status": "StreamTools TikTok Server activo"}

@app.head("/")
def root_head():
    return {}

@app.get("/userinfo")
async def userinfo(username: str):
    username = username.strip().lstrip("@")
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(
                f"https://www.tiktok.com/@{username}",
                headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
                    "Accept-Language": "en-US,en;q=0.9"
                }
            )
            html = resp.text
            followers = -1
            likes = -1
            following = -1
            nickname = username
            avatar_url = ""
            is_live = False

            matches = re.findall(r'"followerCount"\s*:\s*(\d+)', html)
            if matches:
                followers = int(matches[0])
            matches = re.findall(r'"heartCount"\s*:\s*(\d+)', html)
            if matches:
                likes = int(matches[0])
            matches = re.findall(r'"followingCount"\s*:\s*(\d+)', html)
            if matches:
                following = int(matches[0])
            m = re.search(r'"nickname"\s*:\s*"([^"]+)"', html)
            if m:
                nickname = m.group(1)
            m = re.search(r'"avatarLarger"\s*:\s*"([^"]+)"', html)
            if not m:
                m = re.search(r'"avatarMedium"\s*:\s*"([^"]+)"', html)
            if m:
                avatar_url = m.group(1).replace("\\u002F", "/").replace("\\/", "/")
            is_live = bool(re.search(r'"isLiving"\s*:\s*true', html)) or \
                      bool(re.search(r'"roomId"\s*:\s*"(\d{10,})"', html))

            return {
                "username": username,
                "nickname": nickname,
                "followerCount": followers,
                "heartCount": likes,
                "followingCount": following,
                "avatarUrl": avatar_url,
                "isLive": is_live
            }
    except Exception as e:
        return {
            "username": username,
            "followerCount": -1,
            "heartCount": -1,
            "followingCount": -1,
            "avatarUrl": "",
            "isLive": False,
            "error": str(e)
        }

async def resolver_usuario(input_str: str) -> str:
    input_str = input_str.strip()
    if not input_str.startswith("http"):
        return input_str.lstrip("@").strip()
    if "tiktok.com/@" in input_str:
        try:
            parte = input_str.split("tiktok.com/@")[1]
            return parte.split("/")[0].split("?")[0].strip()
        except:
            return ""
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            resp = await client.get(input_str, headers={
                "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"
            })
            final_url = str(resp.url)
            if "tiktok.com/@" in final_url:
                parte = final_url.split("tiktok.com/@")[1]
                return parte.split("/")[0].split("?")[0].strip()
    except:
        pass
    return ""

_sesiones_activas: dict = {}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    params  = websocket.query_params
    entrada = params.get("username", "").strip()
    palabra = params.get("keyword", "").strip().lower()
    sessionid = params.get("sessionid", "").strip()

    if palabra in [".", ",", " ", ""]:
        palabra = ""

    usuario = await resolver_usuario(entrada)
    logger.info(f"[WS] Conectando usuario='{usuario}' keyword='{palabra}'")

    if usuario in _sesiones_activas:
        try:
            _sesiones_activas[usuario].set()
        except:
            pass

    if not usuario:
        try:
            await websocket.send_text(json.dumps({"error": "No se pudo obtener el usuario."}))
        except:
            pass
        await websocket.close()
        return

    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent

    client = TikTokLiveClient(unique_id=usuario)
    if sessionid:
        try:
            client.web.cookies.set("sessionid", sessionid, domain=".tiktok.com")
        except:
            pass

    stop_event = asyncio.Event()
    _sesiones_activas[usuario] = stop_event

    mensajes_vistos = {}

    def ya_procesado(msg_id: str) -> bool:
        ahora = time.time()
        viejos = [k for k, t in mensajes_vistos.items() if ahora - t > 60]
        for k in viejos:
            del mensajes_vistos[k]
        if msg_id in mensajes_vistos:
            return True
        mensajes_vistos[msg_id] = ahora
        return False

    @client.on(ConnectEvent)
    async def on_connect(event):
        logger.info(f"[TikTok] Conectado al live de '{usuario}'")
        try:
            await websocket.send_text(json.dumps({"type": "connected", "usuario": usuario}))
        except:
            pass

    @client.on(CommentEvent)
    async def on_comment(event):
        try:
            comentario = event.comment or ""
            msg_id = f"chat_{event.user.unique_id}_{comentario}"
            if ya_procesado(msg_id):
                return
            if palabra:
                if palabra not in comentario.lower().split():
                    return
            await websocket.send_text(json.dumps({
                "type": "chat",
                "uniqueId": event.user.unique_id,
                "comment": comentario
            }))
        except:
            pass

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        logger.info(f"[TikTok] Desconectado del live de '{usuario}'")
        stop_event.set()
        try:
            await websocket.send_text(json.dumps({"type": "disconnected"}))
        except:
            pass

    async def tiktok_runner():
        try:
            await client.start()
        except Exception as e:
            logger.error(f"[TikTok] ERROR conectando a '{usuario}': {type(e).__name__}: {e}")
            try:
                await websocket.send_text(json.dumps({"type": "error", "error": str(e)}))
            except:
                pass
        finally:
            stop_event.set()

    async def ping_loop():
        async def recibir():
            while not stop_event.is_set():
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=180)
                    if data == "ping":
                        await websocket.send_text("pong")
                except asyncio.TimeoutError:
                    logger.info(f"[WS] Timeout cliente '{usuario}'")
                    stop_event.set()
                    break
                except WebSocketDisconnect:
                    logger.info(f"[WS] App desconectó '{usuario}'")
                    stop_event.set()
                    break
                except Exception as e:
                    stop_event.set()
                    break

        async def enviar_pings():
            while not stop_event.is_set():
                await asyncio.sleep(20)
                if stop_event.is_set():
                    break
                try:
                    await websocket.send_text("ping")
                except:
                    stop_event.set()
                    break

        await asyncio.gather(recibir(), enviar_pings())

    try:
        await asyncio.gather(tiktok_runner(), ping_loop())
    finally:
        if usuario and _sesiones_activas.get(usuario) is stop_event:
            del _sesiones_activas[usuario]
        try:
            await client.stop()
        except:
            pass
        try:
            await websocket.close()
        except:
            pass
        logger.info(f"[WS] Sesión terminada para '{usuario}'")
