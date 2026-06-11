from pathlib import Path

import bpy
from mathutils import Vector


OUT_BLEND = Path(r"C:\Users\souds\Desktop\Untitled_pescoco_reforcado.blend")

# Medidas levantadas do modelo atual. A peça tem cerca de 30 unidades de altura.
NECK_CENTER_X = 6.82
NECK_CENTER_Y = 6.84
NECK_CENTER_Z = 19.55
NECK_HEIGHT = 2.35
NECK_RADIUS_X = 0.78
NECK_RADIUS_Y = 0.66


def get_main_mesh():
    meshes = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
    if not meshes:
        raise RuntimeError("Nenhum objeto de malha encontrado.")
    return max(meshes, key=lambda obj: len(obj.data.polygons))


main_obj = get_main_mesh()

bpy.ops.object.select_all(action="DESELECT")

# Reforco estrutural: um cilindro oval, levemente chanfrado, que fica
# sobreposto ao pescoco original para aumentar a area resistente na impressao.
bpy.ops.mesh.primitive_cylinder_add(
    vertices=128,
    radius=1.0,
    depth=NECK_HEIGHT,
    end_fill_type="NGON",
    location=(NECK_CENTER_X, NECK_CENTER_Y, NECK_CENTER_Z),
)
support = bpy.context.object
support.name = "Reforco_pescoco_impressao"
support.data.name = "Reforco_pescoco_impressao_mesh"
support.scale.x = NECK_RADIUS_X
support.scale.y = NECK_RADIUS_Y
bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)

mat = bpy.data.materials.new("Reforco_pescoco_cinza")
mat.diffuse_color = (0.62, 0.62, 0.62, 1.0)
support.data.materials.append(mat)

bevel = support.modifiers.new("Arredondar bordas do reforco", "BEVEL")
bevel.width = 0.16
bevel.segments = 8
bevel.affect = "EDGES"

support.modifiers.new("Suavizar reforco", "WEIGHTED_NORMAL")

bpy.context.view_layer.objects.active = support
support.select_set(True)
bpy.ops.object.shade_smooth()

# Deixa o objeto principal selecionado junto com o reforco para facilitar a revisao.
main_obj.select_set(True)
support.select_set(True)
bpy.context.view_layer.objects.active = support

# Adiciona uma anotacao simples como propriedade customizada, sem alterar a malha original.
support["observacao"] = (
    "Reforco leve do pescoco para impressao 3D. "
    "O objeto original foi preservado; revise visualmente antes de exportar."
)

bpy.ops.wm.save_as_mainfile(filepath=str(OUT_BLEND))
print(f"SAVED_REINFORCED_COPY={OUT_BLEND}")
