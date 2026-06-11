import bpy


OUT_BLEND = r"C:\Users\souds\Desktop\Untitled_pescoco_controle_360.blend"

bpy.ops.preferences.addon_enable(module="codex_neck_reinforcer")
bpy.ops.codex.create_update_neck_reinforcement()
bpy.ops.wm.save_as_mainfile(filepath=OUT_BLEND)
print(f"SAVED_CONTROLLED_NECK_REINFORCEMENT={OUT_BLEND}")
