import http from "node:http";
import path from "node:path";

import express from "express";
import { Server } from "socket.io";

import { kafkaClient } from "./kafka-client.js";

async function main() {
  const PORT = process.env.PORT ?? 8000;

  const app = express();
  const server = http.createServer(app);
  const io = new Server();

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  io.attach(server);

  io.on("connection", (socket) => {
    console.log(`[Socket:${socket.id}]: Connected Success...`);

    socket.on("client:location:update", async (locationData) => {
      const { latitude, longitude } = locationData;
      console.log(
        `[Socket:${socket.id}]:client:location:update:`,
        locationData,
      );

      await kafkaProducer.send({
        topic: "location-updates",
        messages: [
          {
            key: socket.id,
            value: JSON.stringify({ id: socket.id, latitude, longitude }),
          },
        ],
      });
    });
  });

  app.use(express.static(path.resolve("./public")));

  app.get("/health", (req, res) => {
    return res.json({ healthy: true });
  });

  server.listen(PORT, () => {
    console.log(`Server runing on http://localhost:${PORT}`);
  });
}

main();
