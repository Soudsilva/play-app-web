import bpy


obj = next((o for o in bpy.context.scene.objects if o.type == "MESH"), None)
if obj is None:
    raise SystemExit("No mesh object found")

verts = [obj.matrix_world @ v.co for v in obj.data.vertices]
for z0, z1 in [(18.6, 20.5), (19.4, 20.5), (19.8, 20.4)]:
    band = [v for v in verts if z0 <= v.z <= z1]
    if not band:
        continue
    print(f"band={z0:.1f}-{z1:.1f}")
    print(f"  count={len(band)}")
    print(f"  center={sum(v.x for v in band)/len(band):.3f},{sum(v.y for v in band)/len(band):.3f},{sum(v.z for v in band)/len(band):.3f}")
    print(f"  bounds_x={min(v.x for v in band):.3f},{max(v.x for v in band):.3f}")
    print(f"  bounds_y={min(v.y for v in band):.3f},{max(v.y for v in band):.3f}")
    print(f"  bounds_z={min(v.z for v in band):.3f},{max(v.z for v in band):.3f}")
