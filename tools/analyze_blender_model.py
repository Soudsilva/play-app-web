import bpy


def fmt_vec(values):
    return ", ".join(f"{v:.3f}" for v in values)


print("BLENDER_MODEL_ANALYSIS_START")
print(f"file={bpy.data.filepath}")

for obj in bpy.context.scene.objects:
    if obj.type != "MESH":
        continue

    mesh = obj.data
    tri_faces = sum(1 for poly in mesh.polygons if len(poly.vertices) == 3)
    quad_faces = sum(1 for poly in mesh.polygons if len(poly.vertices) == 4)
    other_faces = len(mesh.polygons) - tri_faces - quad_faces

    print(f"object={obj.name}")
    print(f"  vertices={len(mesh.vertices)}")
    print(f"  edges={len(mesh.edges)}")
    print(f"  faces={len(mesh.polygons)}")
    print(f"  triangles={tri_faces}")
    print(f"  quads={quad_faces}")
    print(f"  other_faces={other_faces}")
    print(f"  dimensions={fmt_vec(obj.dimensions)}")
    print(f"  location={fmt_vec(obj.location)}")

print("BLENDER_MODEL_ANALYSIS_END")
