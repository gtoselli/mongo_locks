import { assertRejects } from "https://deno.land/std@0.224.0/testing/asserts.ts";
import { MongoClient, ObjectId } from "npm:mongodb@6.1.0";
import { mongoLocks } from "./main.ts";

const client = new MongoClient("mongodb://root:root@127.0.0.1:27017");
await client.connect();

const db = client.db("locks_demo");
const collection = db.collection("locks");
const locks = mongoLocks(collection);

Deno.test("should throw if lock already acquired", async () => {
  try {
    const key = new ObjectId();
    await locks.acquireLock(key);

    await assertRejects(
      () => locks.acquireLock(key),
      Error,
      "Lock already acquired",
    );
  } finally {
    await collection.deleteMany({});
  }
});

Deno.test("should not throw if acquired lock is released", async () => {
  try {
    const key = new ObjectId();
    await locks.acquireLock(key);
    await locks.releaseLock(key);

    await locks.acquireLock(key);
  } finally {
    await collection.deleteMany({});
  }
});

Deno.test("should not throw if acquired lock is expired", async () => {
  try {
    const key = new ObjectId();
    await locks.acquireLock(key, 30);

    makeSecondsPass(30);
    await locks.acquireLock(key);
  } finally {
    await collection.deleteMany({});
    restoreDateNow();
  }
});

let originalDateNow: () => number;
export function makeSecondsPass(secondsToAdd: number): void {
  originalDateNow = Date.now;
  const fakeNow = Date.now() + secondsToAdd * 1000;
  Date.now = () => fakeNow;
}
export function restoreDateNow(): void {
  if (originalDateNow) {
    Date.now = originalDateNow;
  }
}
