import type { Context } from "hono";

export const errorHandler = async (err: Error, c: Context) => {
  return c.json(
    {
      success: false,
      error: err.message || "Internal Server Error",
    },
    500
  );
};
