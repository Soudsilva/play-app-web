import bpy


OUT_BLEND = r"C:\Users\souds\Desktop\Untitled_pescoco_controle_360_diminuir.blend"

bpy.ops.preferences.addon_enable(module="codex_neck_reinforcer")
bpy.ops.codex.create_update_neck_reinforcement()
bpy.ops.codex.create_update_neck_shortening()
bpy.ops.wm.save_as_mainfile(filepath=OUT_BLEND)
print(f"SAVED_CONTROLLED_NECK_REINFORCEMENT_AND_SHORTENING={OUT_BLEND}")
