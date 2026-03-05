import pandas as pd
import json
import os

payload_file = r'C:\Users\ITGUSAP\Downloads\scu200hs_lora_payload_2025-09-29_09-48-56_UTC.csv'
variables_file = r'C:\Users\ITGUSAP\Downloads\scu200hs_variables_list_2025-09-29_09-48-56_UTC.csv'

payload_df = pd.read_csv(payload_file, sep=";")
variables_df = pd.read_csv(variables_file, sep=";")

payload_df.columns = [c.strip().lower() for c in payload_df.columns]
variables_df.columns = [c.strip().lower() for c in variables_df.columns]

common_cols = set(payload_df.columns).intersection(set(variables_df.columns))
if not common_cols:
    raise ValueError("Can't perform merge")
merge_key = list(common_cols)[0]
df = pd.merge(payload_df, variables_df, on=merge_key, how="inner")

frames = {}
for _, row in df.iterrows():
    frame_id = int(row["frameid"])
    device = row["devicetype"]
    if frame_id not in frames:
        frames[frame_id] = {"device": device, "vars": [], "current_offset": 2}
    dtype = row["datatype"].lower()
    if dtype == "integer":
        dtype = "signed"
    elif dtype == "boolean":
        dtype = "boolean"
    size = int(row["datasize[bytes]"])
    offset = frames[frame_id]["current_offset"]
    frames[frame_id]["vars"].append({
        "name": row["variable"],
        "type": dtype,
        "size": size,
        "offset": offset,
        "multiplier": float(row.get("multiplier", 1)) if not pd.isna(row.get("multiplier")) else 1,
        "unit": row.get("unit", "")
    })
    frames[frame_id]["current_offset"] += size

def conversion_func(v):
    if v["type"] == "unsigned":
        return "convertUnsigned"
    elif v["type"] == "signed":
        return "convertSigned"
    elif v["type"] == "boolean":
        return "convertBoolean"
    else:
        return "convertSigned"

decoder = """
function convertUnsigned(bytes, start, length) {
  let value = 0;
  for (let i = 0; i < length; i++) {
    value = (value << 8) | bytes[start + i];
  }
  return value;
}
function convertSigned(bytes, start, length) {
  let value = 0;
  for (let i = 0; i < length; i++) {
    value = (value << 8) | bytes[start + i];
  }
  let edge = 1 << (length * 8);
  let max = 1 << ((length * 8) - 1);
  if (value >= max) value -= edge;
  return value;
}
function convertBoolean(bytes, start, length) {
  let value = 0;
  for (let i = 0; i < length; i++) {
    value = (value << 8) | bytes[start + i];
  }
  return value === 1 ? "Closed" : "Open";
}

function decodeUplink(input) {
  const bytes = input.bytes;
  const frameNumber = (bytes[0] << 8) | bytes[1];
  let decoded = { 
    "Frame number": frameNumber,
    "Total bytes": bytes.length,
  };

  switch (frameNumber) {
"""

for frame_id, data in frames.items():
    decoder += f'    case {frame_id}:\n'
    decoder += f'      decoded["{data["device"]}"] = {{\n'
    for v in data["vars"]:
        conv = conversion_func(v)
        mult = f" / {v['multiplier']}" if v["multiplier"] != 1 else ""
        unit = f" [{v['unit']}]" if v["unit"] else ""
        decoder += f'        "{v["name"]}{unit}": {conv}(bytes, {v["offset"]}, {v["size"]}){mult},\n'
    decoder += "      };\n"
    decoder += "      break;\n\n"

decoder += """    default:
      decoded.error = "Unknown frame";
      break;
  }
  return { data: decoded };
}
"""

downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
output_path = os.path.join(downloads_path, "decoder.js")

with open(output_path, "w", encoding="utf-8") as f:
    f.write(decoder)

print(f"Formatter successfully exported to: {output_path}")