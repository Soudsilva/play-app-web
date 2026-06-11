from pathlib import Path

import bpy


OUT_BLEND = Path(r"C:\Users\souds\Desktop\Untitled_pescoco_reforcado_visivel.blend")

# Segunda versao: reforco mais externo e perceptivel para impressao.
# Mantem o original intacto e cria um objeto separado, sobreposto ao pescoco.
NECK_CENTER_X = 6.82
NECK_CENTER_Y = 6.86
NECK_CENTER_Z = 19.55
NECK_HEIGHT = 2.65
NECK_RADIUS_X = 1.08
NECK_RADIUS_Y = 0.92


def get_main_mesh():
    meshes = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
    if not meshes:
        raise RuntimeError("Nenhum objeto de malha encontrado.")
    return max(meshes, key=lambda obj: len(obj.data.polygons))


main_obj = get_main_mesh()

bpy.ops.object.select_all(action="DESELECT")

bpy.ops.mesh.primitive_cylinder_add(
    vertices=160,
    radius=1.0,
    depth=NECK_HEIGHT,
    end_fill_type="NGON",
    location=(NECK_CENTER_X, NECK_CENTER_Y, NECK_CENTER_Z),
)
support = bpy.context.object
support.name = "Reforco_pescoco_visivel_impressao"
support.data.name = "Reforco_pescoco_visivel_impressao_mesh"
support.scale.x = NECK_RADIUS_X
support.scale.y = NECK_RADIUS_Y
bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)

mat = bpy.data.materials.new("Reforco_pescoco_visivel_cinza")
mat.diffuse_color = (0.62, 0.62, 0.62, 1.0)
support.data.materials.append(mat)

bevel = support.modifiers.new("Arredondar transicoes", "BEVEL")
bevel.width = 0.22
bevel.segments = 10
bevel.affect = "EDGES"

support.modifiers.new("Normal suave", "WEIGHTED_NORMAL")

bpy.context.view_layer.objects.active = support
support.select_set(True)
bpy.ops.object.shade_smooth()

main_obj.select_set(True)
support.select_set(True)
bpy.context.view_layer.objects.active = support

support["observacao"] = (
    "Reforco mais visivel do pescoco para impressao 3D. "
    "Se a aparencia ficar grossa demais, reduzir raio X/Y antes de exportar."
)

bpy.ops.wm.save_as_mainfile(filepath=str(OUT_BLEND))
print(f"SAVED_VISIBLE_REINFORCED_COPY={OUT_BLEND}")
