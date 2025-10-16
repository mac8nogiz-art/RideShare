import WebSocket from "ws";

// connect to correct Elysia WebSocket route
const ws = new WebSocket("ws://localhost:3003/driver/location");

ws.on("open", () => {
    console.log(" Connected to WebSocket");

    // Send fake driver location every 5 seconds
    setInterval(() => {
        const payload = {
            driverId: "driver_101",
            lat: 28.6139 + Math.random() * 0.01,
            lng: 77.2090 + Math.random() * 0.01,
            timestamp: Date.now(),
        };

        ws.send(JSON.stringify(payload));
        console.log(" Sent location:", payload);
    }, 5000);
});

ws.on("message", (data: any ) => {
    console.log("📥 From server:", data.toString());
});

ws.on("error", (err: Error) => {
    console.error(" WS error:", err.message);
});

ws.on("close", () => {
    console.log("🔌 Connection closed");
});
