import math

import bpy


obj = next((o for o in bpy.context.scene.objects if o.type == "MESH"), None)
if obj is None:
    raise SystemExit("No mesh object found")

world_vertices = [obj.matrix_world @ v.co for v in obj.data.vertices]
min_z = min(v.z for v in world_vertices)
max_z = max(v.z for v in world_vertices)
center_x = sum(v.x for v in world_vertices) / len(world_vertices)
center_y = sum(v.y for v in world_vertices) / len(world_vertices)

print("NECK_SLICE_ANALYSIS_START")
print(f"object={obj.name}")
print(f"z_range={min_z:.3f},{max_z:.3f}")
print(f"center_avg={center_x:.3f},{center_y:.3f}")

bins = 80
for i in range(bins):
    z0 = min_z + (max_z - min_z) * i / bins
    z1 = min_z + (max_z - min_z) * (i + 1) / bins
    verts = [v for v in world_vertices if z0 <= v.z < z1]
    if len(verts) < 20:
        continue
    xw = max(v.x for v in verts) - min(v.x for v in verts)
    yw = max(v.y for v in verts) - min(v.y for v in verts)
    radial = [math.hypot(v.x - center_x, v.y - center_y) for v in verts]
    p80 = sorted(radial)[int(len(radial) * 0.8)]
    print(f"slice z={((z0+z1)/2):.3f} count={len(verts)} xw={xw:.3f} yw={yw:.3f} r80={p80:.3f}")

print("NECK_SLICE_ANALYSIS_END")
