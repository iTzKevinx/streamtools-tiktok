import asyncio
import json
import os
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
                headers={"User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}
            )
            html = resp.text
            # Extraer stats del JSON embebido en el HTML
            import re
            m = re.search(r'"followerCount":(\d+)', html)
            followers = int(m.group(1)) if m else -1
            m = re.search(r'"heartCount":(\d+)', html)
            likes = int(m.group(1)) if m else -1
            m = re.search(r'"followingCount":(\d+)', html)
            following = int(m.group(1)) if m else -1
            m = re.search(r'"nickname":"([^"]+)"', html)
            nickname = m.group(1) if m else username
            return {
                "username": username,
                "nickname": nickname,
                "followerCount": followers,
                "heartCount": likes,
                "followingCount": following
            }
    except Exception as e:
        return {"username": username, "followerCount": -1, "heartCount": -1, "followingCount": -1, "error": str(e)}

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
    except Exception as e:
        pass
    return ""

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    params  = websocket.query_params
    entrada = params.get("username", "").strip()
    palabra = params.get("keyword", "").strip().lower()
    # Modo alertas: si se envía alerts=true, enviar eventos follow y gift
    modo_alertas = params.get("alerts", "false").strip().lower() == "true"

    usuario = await resolver_usuario(entrada)

    if not usuario:
        await websocket.send_text(json.dumps({"error": "No se pudo obtener el usuario. Usa @usuario directamente."}))
        await websocket.close()
        return

    client = TikTokLiveClient(unique_id=usuario)

    @client.on(ConnectEvent)
    async def on_connect(event):
        try:
            await websocket.send_text(json.dumps({"type": "connected", "usuario": usuario}))
        except:
            pass

    @client.on(CommentEvent)
    async def on_comment(event):
        try:
            mensaje = event.comment.lower() if event.comment else ""
            import re
            palabras = mensaje.split()
            if not palabra or palabra in palabras:
                await websocket.send_text(json.dumps({
                    "type": "chat",
                    "uniqueId": event.user.unique_id,
                    "comment": event.comment
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
            # Solo enviar cuando el regalo está completo (evitar eventos parciales)
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
        try:
            await websocket.send_text(json.dumps({"type": "disconnected"}))
        except:
            pass

    try:
        await client.start()
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
    except Exception as e:
        try:
            await websocket.send_text(json.dumps({"error": str(e)}))
        except:
            pass
    finally:
        try:
            await client.stop()
        except:
            pass
        try:
            await websocket.close()
        except:
            pass
