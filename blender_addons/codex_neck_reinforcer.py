bl_info = {
    "name": "Codex - Reforco 360 do Pescoco",
    "author": "Codex",
    "version": (1, 2, 0),
    "blender": (5, 1, 0),
    "location": "View3D > Sidebar > Codex",
    "description": "Cria reforco circular no pescoco e reducao reversivel de malha.",
    "category": "Object",
}

import bpy


SUPPORT_NAME = "Codex_Reforco_Pescoco_360"
MATERIAL_NAME = "Codex_Reforco_Pescoco_360_Cinza"
DECIMATE_MODIFIER_NAME = "Codex_Reduzir_Malha"


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


def create_or_update_mesh_reduction(context):
    props = context.scene.codex_neck_reinforcer
    obj = _get_main_mesh_object()
    if obj is None:
        return None

    modifier = obj.modifiers.get(DECIMATE_MODIFIER_NAME)
    if modifier is None:
        modifier = obj.modifiers.new(DECIMATE_MODIFIER_NAME, "DECIMATE")

    modifier.decimate_type = "COLLAPSE"
    modifier.ratio = props.mesh_ratio
    modifier.use_collapse_triangulate = False
    modifier.show_viewport = props.mesh_preview
    modifier.show_render = props.mesh_preview

    obj["codex_reducao_malha"] = (
        "Modificador Decimate reversivel criado pelo Codex. "
        "Aplique o modificador somente em uma copia final para exportacao."
    )
    return obj


def remove_mesh_reduction():
    obj = _get_main_mesh_object()
    if obj is None:
        return False
    modifier = obj.modifiers.get(DECIMATE_MODIFIER_NAME)
    if modifier is None:
        return False
    obj.modifiers.remove(modifier)
    return True


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
    mesh_ratio: bpy.props.FloatProperty(
        name="Quantidade de malha",
        description="1.00 mantem tudo; 0.70 preserva cerca de 70%; 0.50 preserva cerca de metade",
        default=0.70,
        min=0.10,
        max=1.00,
        step=5,
        precision=2,
    )
    mesh_preview: bpy.props.BoolProperty(
        name="Mostrar reducao",
        description="Liga ou desliga a visualizacao da reducao sem remover o ajuste",
        default=True,
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


class CODEX_OT_create_update_mesh_reduction(bpy.types.Operator):
    bl_idname = "codex.create_update_mesh_reduction"
    bl_label = "Atualizar Reducao"
    bl_description = "Cria ou atualiza um modificador Decimate reversivel na peca inteira"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        obj = create_or_update_mesh_reduction(context)
        if obj is None:
            self.report({"WARNING"}, "Objeto principal do personagem nao encontrado.")
            return {"CANCELLED"}
        return {"FINISHED"}


class CODEX_OT_remove_mesh_reduction(bpy.types.Operator):
    bl_idname = "codex.remove_mesh_reduction"
    bl_label = "Remover Reducao"
    bl_description = "Remove o modificador de reducao da malha"
    bl_options = {"REGISTER", "UNDO"}

    def execute(self, context):
        if not remove_mesh_reduction():
            self.report({"WARNING"}, "A reducao de malha ainda nao existe.")
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
        advanced.label(text="Detalhe do reforco")
        advanced.prop(props, "segments")
        advanced.prop(props, "bevel_segments")

        layout.operator("codex.create_update_neck_reinforcement", icon="MOD_SOLIDIFY")
        row = layout.row(align=True)
        row.operator("codex.select_neck_reinforcement", icon="RESTRICT_SELECT_OFF")
        row.operator("codex.remove_neck_reinforcement", icon="TRASH")

        reduction = layout.box()
        reduction.label(text="Reduzir malha")
        reduction.prop(props, "mesh_ratio")
        reduction.prop(props, "mesh_preview")
        reduction.operator("codex.create_update_mesh_reduction", icon="MOD_DECIM")
        reduction.operator("codex.remove_mesh_reduction", icon="TRASH")


classes = (
    CodexNeckReinforcerProperties,
    CODEX_OT_create_update_neck_reinforcement,
    CODEX_OT_remove_neck_reinforcement,
    CODEX_OT_select_neck_reinforcement,
    CODEX_OT_create_update_mesh_reduction,
    CODEX_OT_remove_mesh_reduction,
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
