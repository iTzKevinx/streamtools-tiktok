import asyncio
import json
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent

app = FastAPI()

@app.get("/")
def root():
    return {"status": "StreamTools TikTok Server activo"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    params = websocket.query_params
    usuario   = params.get("username", "").strip().lstrip("@")
    palabra   = params.get("keyword", "").strip().lower()

    if not usuario:
        await websocket.send_text(json.dumps({"error": "Usuario requerido"}))
        await websocket.close()
        return

    client = TikTokLiveClient(unique_id=usuario)

    @client.on(ConnectEvent)
    async def on_connect(event):
        try:
            await websocket.send_text(json.dumps({"type": "connected"}))
        except Exception:
            pass

    @client.on(CommentEvent)
    async def on_comment(event):
        try:
            mensaje = event.comment.lower() if event.comment else ""
            if not palabra or palabra in mensaje:
                await websocket.send_text(json.dumps({
                    "type": "chat",
                    "uniqueId": event.user.unique_id,
                    "comment": event.comment
                }))
        except Exception:
            pass

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        try:
            await websocket.send_text(json.dumps({"type": "disconnected"}))
        except Exception:
            pass

    try:
        await client.start()
        # Mantener la conexión activa
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
        except Exception:
            pass
    finally:
        try:
            await client.stop()
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass
