import bpy


bpy.ops.preferences.addon_enable(module="codex_neck_reinforcer")
bpy.ops.wm.save_userpref()
print("CODEX_NECK_REINFORCER_ENABLED")
