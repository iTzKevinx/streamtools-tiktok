import asyncio
import json
import logging
import os
import re
import time
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent, FollowEvent, GiftEvent

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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    params       = websocket.query_params
    entrada      = params.get("username", "").strip()
    palabra      = params.get("keyword", "").strip().lower()
    modo_alertas = params.get("alerts", "false").strip().lower() == "true"
    sessionid    = params.get("sessionid", "").strip()

    # Limpiar keyword inválido (punto solo, espacios, etc.)
    if palabra in [".", ",", " ", ""]:
        palabra = ""

    usuario = await resolver_usuario(entrada)
    logger.info(f"[WS] Conectando usuario='{usuario}' keyword='{palabra}' alerts={modo_alertas}")

    if not usuario:
        await websocket.send_text(json.dumps({"error": "No se pudo obtener el usuario."}))
        await websocket.close()
        return

    client = TikTokLiveClient(
        unique_id=usuario,
        **({"session_id": sessionid} if sessionid else {})
    )
    stop_event = asyncio.Event()
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

    @client.on(FollowEvent)
    async def on_follow(event):
        if not modo_alertas:
            return
        try:
            msg_id = f"follow_{event.user.unique_id}_{int(time.time() // 10)}"
            if ya_procesado(msg_id):
                return
            await websocket.send_text(json.dumps({
                "type": "follow",
                "uniqueId": event.user.unique_id,
                "nickname": event.user.nickname or event.user.unique_id
            }))
        except:
            pass

    @client.on(GiftEvent)
    async def on_gift(event):
        if not modo_alertas:
            return
        try:
            if hasattr(event, 'gift') and event.gift is not None:
                if hasattr(event.gift, 'gift_type') and event.gift.gift_type == 1:
                    if hasattr(event, 'repeat_end') and not event.repeat_end:
                        return
            gift_name = event.gift.name if hasattr(event, 'gift') and event.gift else "Gift"
            gift_count = event.repeat_count if hasattr(event, 'repeat_count') else 1
            msg_id = f"gift_{event.user.unique_id}_{gift_name}_{int(time.time() // 5)}"
            if ya_procesado(msg_id):
                return
            await websocket.send_text(json.dumps({
                "type": "gift",
                "uniqueId": event.user.unique_id,
                "nickname": event.user.nickname or event.user.unique_id,
                "giftName": gift_name,
                "giftCount": gift_count
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
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": str(e),
                    "detail": type(e).__name__
                }))
            except:
                pass
        finally:
            stop_event.set()

    async def ping_loop():
        while not stop_event.is_set():
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                try:
                    await websocket.send_text("pong")
                except:
                    stop_event.set()
                    break
            except WebSocketDisconnect:
                logger.info(f"[WS] App desconectó el WebSocket de '{usuario}'")
                stop_event.set()
                break
            except Exception as e:
                logger.error(f"[WS] Error en ping_loop: {e}")
                stop_event.set()
                break

    try:
        await asyncio.gather(tiktok_runner(), ping_loop())
    finally:
        try:
            await client.stop()
        except:
            pass
        try:
            await websocket.close()
        except:
            pass
        logger.info(f"[WS] Sesión terminada para '{usuario}'")
