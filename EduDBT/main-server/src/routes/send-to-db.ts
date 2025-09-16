import { Hono } from "hono";

const sendToDb = new Hono();

sendToDb.post("/send-to-db", async (c) => {
  try {
    const body = await c.req.json();

    const { name, email, image } = body;

    console.log("Received user from Next.js:", { name, email, image });

    // Forward to Go server
    const goRes = await fetch("http://localhost:3200/save-user", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, email, image }),
    });

    const goData = await goRes.json();

    return c.json({ success: true, goResponse: goData });
  } catch (err) {
    console.error("Error in HonoJS:", err);
    return c.json({ error: "Failed to process request" }, 500);
  }
});

export default sendToDb;
