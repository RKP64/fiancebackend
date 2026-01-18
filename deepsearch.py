import time
from google import genai

client = genai.Client(api_key="AIzaSyDfok3taVXUnPqIci34TN9kWlhFoee28ps")

print("🚀 Starting Deep Research...")
interaction = client.interactions.create(
    input="Just tell me the name of the drivers in F1 2026 season.",
    agent="deep-research-pro-preview-12-2025",
    background=True,  # NO stream=True - causes errors
)

print(f"🆔 ID: {interaction.id}")
print("⏳⏳⏳ THIS TAKES 10-45 MINUTES - DO NOT INTERRUPT ⏳⏳⏳")

start_time = time.time()
while True:
    interaction = client.interactions.get(interaction.id)
    elapsed = int((time.time() - start_time) / 60)

    print(f"[{elapsed}min] Status: {interaction.status}", end=" ")

    if interaction.status == "completed":
        print("\n✅✅✅ RESEARCH COMPLETE ✅✅✅")
        print("\n" + "=" * 80)
        print(interaction.outputs[-1].text)
        print("=" * 80)
        break

    elif interaction.status == "failed":
        print(f"\n❌ FAILED: {getattr(interaction, 'error', 'Unknown error')}")
        break

    else:
        print()  # New line for next status

    time.sleep(20)  # Check every 20 seconds

print("🎉 Done!")
