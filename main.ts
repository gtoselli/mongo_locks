import { ObjectId, Collection } from "npm:mongodb@6.1.0";

export function mongoLocks(collection: Collection) {
  return {
    async acquireLock(key: string | ObjectId, ttlSeconds = 30) {
      const now = new Date(Date.now());
      const lockExpiresAt = new Date(now.getTime() + ttlSeconds * 1000);

      try {
        await collection.insertOne({
          _id: typeof key === "string" ? new ObjectId(key) : key,
          lockedAt: now,
          lockedUntil: lockExpiresAt,
        });
        return;
      } catch (err: any) {
        if (err.code !== 11000) throw err;

        const result = await collection.updateOne(
          {
            _id: typeof key === "string" ? new ObjectId(key) : key,
            lockedUntil: { $lte: now },
          },
          {
            $set: {
              lockedAt: now,
              lockedUntil: lockExpiresAt,
            },
          },
        );

        if (result.modifiedCount === 0) {
          throw new Error("Lock already acquired");
        }
      }
    },

    async releaseLock(key: string | ObjectId) {
      await collection.deleteOne({
        _id: typeof key === "string" ? new ObjectId(key) : key,
      });
    },
  };
}
