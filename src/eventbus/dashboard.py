import asyncio
import websockets

async def handle_connection(websocket, path):
    while True:
        try:
            # Handle incoming messages
            message = await websocket.recv()
            print(f"Received message: {message}")
            
            # Send response back to client
            response = f"Server received your message: {message}"
            await websocket.send(response)
        except websockets.ConnectionClosed:
            print("Connection closed")
            break

async def main():
    async with websockets.serve(handle_connection, "localhost", 8765):
        print("Server started on port 8765")
        await asyncio.Future()  # run forever

asyncio.run(main())