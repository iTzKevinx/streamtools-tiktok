import asyncio
import json
import os
import re
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent, FollowEvent, GiftEvent

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

    usuario = await resolver_usuario(entrada)
    if not usuario:
        await websocket.send_text(json.dumps({"error": "No se pudo obtener el usuario. Usa @usuario directamente."}))
        await websocket.close()
        return

    client = TikTokLiveClient(unique_id=usuario)
    stop_event = asyncio.Event()

    @client.on(ConnectEvent)
    async def on_connect(event):
        try:
            await websocket.send_text(json.dumps({"type": "connected", "usuario": usuario}))
        except:
            pass

    @client.on(CommentEvent)
    async def on_comment(event):
        try:
            comentario = event.comment or ""
            mensaje = comentario.lower()
            # Si hay keyword, filtrar; si no hay, enviar todos
            if palabra:
                palabras = mensaje.split()
                if palabra not in palabras:
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
            # Solo enviar cuando el regalo está finalizado (no combos intermedios)
            if hasattr(event, 'gift') and event.gift is not None:
                if hasattr(event.gift, 'gift_type') and event.gift.gift_type == 1:
                    if hasattr(event, 'repeat_end') and not event.repeat_end:
                        return
            await websocket.send_text(json.dumps({
                "type": "gift",
                "uniqueId": event.user.unique_id,
                "nickname": event.user.nickname or event.user.unique_id,
                "giftName": event.gift.name if hasattr(event, 'gift') and event.gift else "Gift",
                "giftCount": event.repeat_count if hasattr(event, 'repeat_count') else 1
            }))
        except:
            pass

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        stop_event.set()
        try:
            await websocket.send_text(json.dumps({"type": "disconnected"}))
        except:
            pass

    async def tiktok_runner():
        try:
            await client.start()
        except Exception as e:
            try:
                await websocket.send_text(json.dumps({"error": str(e)}))
            except:
                pass
        finally:
            stop_event.set()

    async def ping_loop():
        """Mantiene el WebSocket vivo respondiendo pings de la app."""
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
                stop_event.set()
                break
            except Exception:
                stop_event.set()
                break

    try:
        # Correr TikTok y ping loop en PARALELO — este era el bug principal
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
