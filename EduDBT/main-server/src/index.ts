import { Hono } from "hono";
import { cors } from "hono/cors";
import { errorHandler } from "@/middleware/error-handler";
import { env } from "@/env"
import sendToDb from "./routes/send-to-db";

const app = new Hono();

app.onError(errorHandler);

app.use("*", cors());

app.get("/", async (c) => c.text("Hono Gateway up 🚀"));
app.route("/", sendToDb);

export default {
  port: env.PORT,
  fetch: app.fetch,
};
