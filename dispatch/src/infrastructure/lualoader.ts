import fs from "fs";
import path from "path";
import { logger } from "../logger";
import Redis from "ioredis";

const luaDir = path.resolve(process.cwd(), "src/lua");

export async function loadLuaScripts(redis: Redis) {
  if (!fs.existsSync(luaDir)) return;

  const files = fs.readdirSync(luaDir).filter((f) => f.endsWith(".lua"));

  for (const file of files) {
    const name = file.replace(".lua", "");
    const script = fs.readFileSync(path.join(luaDir, file), "utf8");
    try {
      (redis as any).defineCommand(name, {
        numberOfKeys: 1,
        lua: script,
      });
      logger.info(`📜 Loaded Lua script: ${name}`);
    } catch (err) {
      logger.error(`Failed to load Lua ${name}: ${(err instanceof Error ? err.message : String(err))}`);
    }
  }
}
