import bpy


bpy.ops.preferences.addon_enable(module="codex_neck_reinforcer")
print("ADDON_ENABLED")
print("HAS_REINFORCE_OPERATOR", hasattr(bpy.ops.codex, "create_update_neck_reinforcement"))
print("HAS_REDUCTION_OPERATOR", hasattr(bpy.ops.codex, "create_update_mesh_reduction"))
bpy.ops.wm.save_userpref()
