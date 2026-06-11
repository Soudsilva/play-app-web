from pathlib import Path

import bpy


ZIP_PATH = Path(r"C:\Users\souds\play-app-web\mcp-blender-addon-fixed.zip")

print(f"INSTALLING_BLENDER_MCP_ADDON={ZIP_PATH}")

try:
    bpy.ops.preferences.addon_install(filepath=str(ZIP_PATH), overwrite=True)
except TypeError:
    bpy.ops.preferences.addon_install(filepath=str(ZIP_PATH))

for module_name in ("mcp", "bl_ext.user_default.mcp"):
    try:
        bpy.ops.preferences.addon_enable(module=module_name)
        print(f"ADDON_ENABLED={module_name}")
        break
    except Exception as exc:
        print(f"ADDON_ENABLE_FAILED={module_name}: {exc}")

prefs = bpy.context.preferences
system = prefs.system

for attr in ("use_online_access", "use_online_access_handled"):
    if hasattr(system, attr):
        try:
            setattr(system, attr, True)
            print(f"SET_SYSTEM_{attr}=True")
        except Exception as exc:
            print(f"SET_SYSTEM_{attr}_FAILED={exc}")

bpy.ops.wm.save_userpref()
print("USER_PREFS_SAVED")
