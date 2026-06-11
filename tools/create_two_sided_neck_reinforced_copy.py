from pathlib import Path

import bpy


OUT_BLEND = Path(r"C:\Users\souds\Desktop\Untitled_pescoco_reforcado_dois_lados.blend")

# Versao com reforco simetrico: um nucleo central e dois volumes laterais
# para aparecer dos dois lados do pescoco na vista frontal.
NECK_CENTER_X = 6.82
NECK_CENTER_Y = 6.86
NECK_CENTER_Z = 19.55
NECK_HEIGHT = 2.65


def get_main_mesh():
    meshes = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
    if not meshes:
        raise RuntimeError("Nenhum objeto de malha encontrado.")
    return max(meshes, key=lambda obj: len(obj.data.polygons))


def add_oval_support(name, location, radius_x, radius_y, height, bevel_width):
    bpy.ops.mesh.primitive_cylinder_add(
        vertices=144,
        radius=1.0,
        depth=height,
        end_fill_type="NGON",
        location=location,
    )
    obj = bpy.context.object
    obj.name = name
    obj.data.name = f"{name}_mesh"
    obj.scale.x = radius_x
    obj.scale.y = radius_y
    bpy.ops.object.transform_apply(location=False, rotation=False, scale=True)

    mat = bpy.data.materials.get("Reforco_pescoco_dois_lados_cinza")
    if mat is None:
        mat = bpy.data.materials.new("Reforco_pescoco_dois_lados_cinza")
        mat.diffuse_color = (0.62, 0.62, 0.62, 1.0)
    obj.data.materials.append(mat)

    bevel = obj.modifiers.new("Arredondar transicoes", "BEVEL")
    bevel.width = bevel_width
    bevel.segments = 10
    bevel.affect = "EDGES"
    obj.modifiers.new("Normal suave", "WEIGHTED_NORMAL")

    bpy.context.view_layer.objects.active = obj
    obj.select_set(True)
    bpy.ops.object.shade_smooth()
    obj.select_set(False)
    return obj


main_obj = get_main_mesh()
bpy.ops.object.select_all(action="DESELECT")

supports = [
    add_oval_support(
        "Reforco_pescoco_central",
        (NECK_CENTER_X, NECK_CENTER_Y, NECK_CENTER_Z),
        0.88,
        0.78,
        NECK_HEIGHT,
        0.20,
    ),
    add_oval_support(
        "Reforco_pescoco_lado_esquerdo",
        (NECK_CENTER_X - 0.52, NECK_CENTER_Y, NECK_CENTER_Z),
        0.44,
        0.66,
        NECK_HEIGHT * 0.96,
        0.16,
    ),
    add_oval_support(
        "Reforco_pescoco_lado_direito",
        (NECK_CENTER_X + 0.52, NECK_CENTER_Y, NECK_CENTER_Z),
        0.44,
        0.66,
        NECK_HEIGHT * 0.96,
        0.16,
    ),
]

for obj in supports:
    obj["observacao"] = (
        "Reforco simetrico do pescoco para impressao 3D. "
        "Objetos separados e sobrepostos para revisao antes de exportar."
    )
    obj.select_set(True)

main_obj.select_set(True)
bpy.context.view_layer.objects.active = supports[0]

bpy.ops.wm.save_as_mainfile(filepath=str(OUT_BLEND))
print(f"SAVED_TWO_SIDED_REINFORCED_COPY={OUT_BLEND}")
