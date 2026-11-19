/** Records go to the host-side collector over cleartext localhost (adb reverse / simulator). */

const COLLECTOR = "http://127.0.0.1:8100";

export async function post(path: string, body: unknown): Promise<void> {
  const response = await fetch(`${COLLECTOR}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`collector ${path} answered ${response.status}`);
  }
}
