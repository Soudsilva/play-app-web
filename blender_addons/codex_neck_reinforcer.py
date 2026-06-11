bl_info = {
    "name": "Codex - Reforco 360 do Pescoco",
    "author": "Codex",
    "version": (1, 1, 0),
    "blender": (5, 1, 0),
    "location": "View3D > Sidebar > Codex",
    "description": "Cria e ajusta um reforco circular 360 graus no pescoco para impressao 3D.",
    "category": "Object",
}

import bpy


SUPPORT_NAME = "Codex_Reforco_Pescoco_360"
MATERIAL_NAME = "Codex_Reforco_Pescoco_360_Cinza"
SHORTEN_SHAPE_KEY_NAME = "Codex_Diminuir_Pescoco"


def _get_support_object():
    return bpy.data.objects.get(SUPPORT_NAME)


def _get_or_create_material():
    material = bpy.data.materials.get(MATERIAL_NAME)
    if material is None:
        material = bpy.data.materials.new(MATERIAL_NAME)
        material.diffuse_color = (0.62, 0.62, 0.62, 1.0)
    return material


def _remove_support():
    obj = _get_support_object()
    if obj is None:
        return
    mesh = obj.data
    bpy.data.objects.remove(obj, do_unlink=True)
    if mesh and mesh.users == 0:
        bpy.data.meshes.remove(mesh)


def _get_main_mesh_object():
    meshes = [
        obj
        for obj in bpy.context.scene.objects
        if obj.type == "MESH" and obj.name != SUPPORT_NAME
    ]
    if not meshes:
        return None
    return max(meshes, key=lambda obj: len(obj.data.polygons))


def _remove_shape_key(obj, key_name):
    if obj is None or obj.data.shape_keys is None:
        return False
    key_blocks = obj.data.shape_keys.key_blocks
    if key_name not in key_blocks:
        return False
    obj.active_shape_key_index = key_blocks.find(key_name)
    bpy.context.view_layer.objects.active = obj
    obj.select_set(True)
    bpy.ops.object.shape_key_remove()
    return True


def create_or_update_neck_shortening(context):
    props = context.scene.codex_neck_reinforcer
    obj = _get_main_mesh_object()
    if obj is None:
        return None

    bpy.ops.object.select_all(action="DESELECT")
    obj.select_set(True)
    context.view_layer.objects.active = obj

    if obj.data.shape_keys is None:
        obj.shape_key_add(name="Basis", from_mix=False)

    _remove_shape_key(obj, SHORTEN_SHAPE_KEY_NAME)
    shape_key = obj.shape_key_add(name=SHORTEN_SHAPE_KEY_NAME, from_mix=False)
    shape_key.value = 1.0

    matrix = obj.matrix_world
    inverse = matrix.inverted()
    bottom_z = props.neck_bottom_z
    top_z = props.neck_top_z
    amount = props.neck_shorten_amount
    band_height = max(top_z - bottom_z, 0.001)

    for vertex in obj.data.vertices:
        world = matrix @ vertex.co
        if world.z <= bottom_z:
            continue
        if world.z >= top_z:
            delta = amount
        else:
            delta = amount * ((world.z - bottom_z) / band_height)
        moved = world.copy()
        moved.z -= delta
        shape_key.data[vertex.index].co = inverse @ moved

    obj["codex_pescoco_diminuido"] = (
        "Shape key reversivel criada pelo Codex para encurtar o pescoco. "
        "Remova a shape key para voltar ao formato original."
    )
    return obj


def create_or_update_neck_reinforcement(context):
    props = context.scene.codex_neck_reinforcer

    _remove_support()

    bpy.ops.mesh.primitive_cylinder_add(
        vertices=props.segments,
        radius=props.radius,
        depth=props.height,
        end_fill_type="NGON",
        location=(props.center_x, props.center_y, props.center_z),
    )

    obj = context.object
    obj.name = SUPPORT_NAME
    obj.data.name = f"{SUPPORT_NAME}_Mesh"
    obj.data.materials.append(_get_or_create_material())

    bevel = obj.modifiers.new("Arredondar bordas", "BEVEL")
    bevel.width = props.bevel
    bevel.segments = props.bevel_segments
    bevel.affect = "EDGES"

    obj.modifiers.new("Normal suave", "WEIGHTED_NORMAL")

    bpy.ops.object.shade_smooth()

    obj["codex_observacao"] = (
        "Reforco circular 360 graus para engrossar o pescoco. "
        "Ajuste raio, altura e posicao antes de exportar para impressao."
    )

    return obj


class CodexNeckReinforcerProperties(bpy.types.PropertyGroup):
    radius: bpy.props.FloatProperty(
        name="Engrossamento",
        description="Raio do reforco circular em volta de todo o pescoco",
        default=1.10,
        min=0.30,
        max=3.00,
        step=5,
        precision=2,
        unit="LENGTH",
    )
    height: bpy.props.FloatProperty(
        name="Altura",
        description="Altura vertical do reforco",
        default=2.65,
        min=0.40,
        max=6.00,
        step=5,
        precision=2,
        unit="LENGTH",
    )
    center_x: bpy.props.FloatProperty(
        name="Centro X",
        description="Centro horizontal do reforco",
        default=6.82,
        min=-100.0,
        max=100.0,
        step=2,
        precision=2,
        unit="LENGTH",
    )
    center_y: bpy.props.FloatProperty(
        name="Centro Y",
        description="Centro de profundidade do reforco",
        default=6.86,
        min=-100.0,
        max=100.0,
        step=2,
        precision=2,
        unit="LENGTH",
    )
    center_z: bpy.props.FloatProperty(
        name="Centro Z",
        description="Altura central do reforco no modelo",
        default=19.55,
        min=-100.0,
        max=100.0,
        step=2,
        precision=2,
        unit="LENGTH",
    )
    bevel: bpy.props.FloatProperty(
        name="Arredondar",
        description="Suaviza as bordas superior e inferior do reforco",
        default=0.20,
        min=0.0,
        max=0.80,
        step=2,
        precision=2,
        unit="LENGTH",
    )
    segments: bpy.props.IntProperty(
        name="Suavidade",
        description="Quantidade de lados do cilindro circular",
        default=160,
        min=32,
        max=256,
    )
    bevel_segments: bpy.props.IntProperty(
        name="Suavidade da borda",
        description="Quantidade de segmentos do arredondamento",
        default=10,
        min=1,
        max=20,
    )
    neck_shorten_amount: bpy.props.FloatProperty(
        name="Diminuir pescoço",
        description="Quanto a cabeça desce e o pescoço encurta",
        default=0.65,
        min=0.0,
        max=2.50,
        step=5,
        precision=2,
        unit="LENGTH",
    )
    neck_bottom_z: bpy.props.FloatProperty(
        name="Base do pescoço",
        description="Altura onde o pescoço fica fixo no corpo",
        default=18.80,
        min=-100.0,
        max=100.0,
        step=2,
        precision=2,
        unit="LENGTH",
    )
    neck_top_z: bpy.props.FloatProperty(
        name="Topo do pescoço",
        description="Altura a partir da qual a cabeça desce junto",
        default=20.70,
        min=-100.0,
        max=100.0,
        step=2,
        precision=2,
        unit="LENGTH",
    )


class CODEX_OT_create_update_neck_reinforcement(bpy.types.Operator):
    bl_idname = "codex.create_update_neck_reinforcement"
    bl_label = "Atualizar Reforco 360"
    bl_description = "Cria ou atualiza o reforco circular em volta de todo o pescoco"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        create_or_update_neck_reinforcement(context)
        return {"FINISHED"}


class CODEX_OT_remove_neck_reinforcement(bpy.types.Operator):
    bl_idname = "codex.remove_neck_reinforcement"
    bl_label = "Remover Reforco"
    bl_description = "Remove o reforco 360 criado pelo Codex"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        _remove_support()
        return {"FINISHED"}


class CODEX_OT_select_neck_reinforcement(bpy.types.Operator):
    bl_idname = "codex.select_neck_reinforcement"
    bl_label = "Selecionar Reforco"
    bl_description = "Seleciona o objeto de reforco para revisao manual"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        obj = _get_support_object()
        if obj is None:
            self.report({"WARNING"}, "Reforco ainda nao foi criado.")
            return {"CANCELLED"}
        bpy.ops.object.select_all(action="DESELECT")
        obj.select_set(True)
        context.view_layer.objects.active = obj
        return {"FINISHED"}


class CODEX_OT_create_update_neck_shortening(bpy.types.Operator):
    bl_idname = "codex.create_update_neck_shortening"
    bl_label = "Aplicar Tamanho do Pescoço"
    bl_description = "Cria ou atualiza uma shape key reversivel para diminuir a altura do pescoco"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        obj = create_or_update_neck_shortening(context)
        if obj is None:
            self.report({"WARNING"}, "Objeto principal do personagem nao encontrado.")
            return {"CANCELLED"}
        return {"FINISHED"}


class CODEX_OT_remove_neck_shortening(bpy.types.Operator):
    bl_idname = "codex.remove_neck_shortening"
    bl_label = "Remover Diminuição"
    bl_description = "Remove a shape key de diminuicao do pescoco"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        obj = _get_main_mesh_object()
        if not _remove_shape_key(obj, SHORTEN_SHAPE_KEY_NAME):
            self.report({"WARNING"}, "A diminuicao do pescoco ainda nao existe.")
            return {"CANCELLED"}
        return {"FINISHED"}


class CODEX_PT_neck_reinforcer_panel(bpy.types.Panel):
    bl_label = "Reforco do Pescoco"
    bl_idname = "CODEX_PT_neck_reinforcer_panel"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "Codex"

    def draw(self, context):
        layout = self.layout
        props = context.scene.codex_neck_reinforcer

        layout.prop(props, "radius")
        layout.prop(props, "height")
        layout.prop(props, "center_x")
        layout.prop(props, "center_y")
        layout.prop(props, "center_z")
        layout.prop(props, "bevel")

        advanced = layout.box()
        advanced.label(text="Detalhe")
        advanced.prop(props, "segments")
        advanced.prop(props, "bevel_segments")

        layout.operator("codex.create_update_neck_reinforcement", icon="MOD_SOLIDIFY")
        row = layout.row(align=True)
        row.operator("codex.select_neck_reinforcement", icon="RESTRICT_SELECT_OFF")
        row.operator("codex.remove_neck_reinforcement", icon="TRASH")

        shorten = layout.box()
        shorten.label(text="Diminuir tamanho")
        shorten.prop(props, "neck_shorten_amount")
        shorten.prop(props, "neck_bottom_z")
        shorten.prop(props, "neck_top_z")
        shorten.operator("codex.create_update_neck_shortening", icon="SHAPEKEY_DATA")
        shorten.operator("codex.remove_neck_shortening", icon="LOOP_BACK")


classes = (
    CodexNeckReinforcerProperties,
    CODEX_OT_create_update_neck_reinforcement,
    CODEX_OT_remove_neck_reinforcement,
    CODEX_OT_select_neck_reinforcement,
    CODEX_OT_create_update_neck_shortening,
    CODEX_OT_remove_neck_shortening,
    CODEX_PT_neck_reinforcer_panel,
)


def register():
    for cls in classes:
        bpy.utils.register_class(cls)
    bpy.types.Scene.codex_neck_reinforcer = bpy.props.PointerProperty(
        type=CodexNeckReinforcerProperties
    )


def unregister():
    if hasattr(bpy.types.Scene, "codex_neck_reinforcer"):
        del bpy.types.Scene.codex_neck_reinforcer
    for cls in reversed(classes):
        bpy.utils.unregister_class(cls)


if __name__ == "__main__":
    register()
