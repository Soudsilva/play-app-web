import math
from pathlib import Path

import bpy
from mathutils import Vector


OUT_DIR = Path(r"C:\Users\souds\play-app-web\.tmp_blender_views")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def mesh_objects():
    return [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]


def scene_bounds(objects):
    corners = []
    for obj in objects:
        corners.extend(obj.matrix_world @ Vector(corner) for corner in obj.bound_box)
    min_v = Vector((min(c.x for c in corners), min(c.y for c in corners), min(c.z for c in corners)))
    max_v = Vector((max(c.x for c in corners), max(c.y for c in corners), max(c.z for c in corners)))
    return min_v, max_v


def look_at(obj, target):
    direction = target - obj.location
    obj.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()


objects = mesh_objects()
min_v, max_v = scene_bounds(objects)
center = (min_v + max_v) * 0.5
size = max((max_v - min_v).x, (max_v - min_v).y, (max_v - min_v).z)

bpy.ops.object.select_all(action="DESELECT")
for obj in objects:
    obj.select_set(True)
    bpy.context.view_layer.objects.active = obj

for obj in objects:
    obj.display_type = "TEXTURED"
    obj.show_wire = False

bpy.context.scene.render.engine = "BLENDER_WORKBENCH"
bpy.context.scene.display.shading.light = "STUDIO"
bpy.context.scene.display.shading.color_type = "MATERIAL"
bpy.context.scene.render.resolution_x = 1400
bpy.context.scene.render.resolution_y = 1400
bpy.context.scene.world.color = (1, 1, 1)

camera = bpy.data.objects.get("Codex_View_Camera")
if camera is None:
    cam_data = bpy.data.cameras.new("Codex_View_Camera")
    camera = bpy.data.objects.new("Codex_View_Camera", cam_data)
    bpy.context.collection.objects.link(camera)
bpy.context.scene.camera = camera
camera.data.type = "ORTHO"
camera.data.ortho_scale = size * 1.15

views = {
    "front": Vector((0, -size * 2.5, 0)),
    "back": Vector((0, size * 2.5, 0)),
    "right": Vector((size * 2.5, 0, 0)),
    "left": Vector((-size * 2.5, 0, 0)),
    "top": Vector((0, 0, size * 2.5)),
}

for name, offset in views.items():
    camera.location = center + offset
    look_at(camera, center)
    bpy.context.scene.render.filepath = str(OUT_DIR / f"{name}.png")
    bpy.ops.render.render(write_still=True)
    print(f"rendered={OUT_DIR / f'{name}.png'}")
