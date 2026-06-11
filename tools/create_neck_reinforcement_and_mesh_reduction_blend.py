import bpy


OUT_BLEND = r"C:\Users\souds\Desktop\Untitled_pescoco_reforco_reduzir_malha.blend"

bpy.ops.preferences.addon_enable(module="codex_neck_reinforcer")
bpy.context.scene.codex_neck_reinforcer.mesh_ratio = 0.70
bpy.context.scene.codex_neck_reinforcer.mesh_preview = False
bpy.ops.codex.create_update_mesh_reduction()
bpy.ops.wm.save_as_mainfile(filepath=OUT_BLEND)
print(f"SAVED_NECK_REINFORCEMENT_AND_MESH_REDUCTION={OUT_BLEND}")
