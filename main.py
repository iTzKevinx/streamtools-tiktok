import asyncio
import json
import os
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent

app = FastAPI()

@app.get("/")
def root():
    return {"status": "StreamTools TikTok Server activo"}

@app.head("/")
def root_head():
    return {}

async def resolver_usuario(input_str: str) -> str:
    input_str = input_str.strip()
    # Si ya es un usuario directo
    if not input_str.startswith("http"):
        return input_str.lstrip("@").strip()
    # Si contiene @usuario en la URL
    if "tiktok.com/@" in input_str:
        try:
            parte = input_str.split("tiktok.com/@")[1]
            return parte.split("/")[0].split("?")[0].strip()
        except:
            return ""
    # Enlace corto — resolver redirección
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
