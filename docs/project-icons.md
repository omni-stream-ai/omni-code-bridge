# Project icons

Project summaries expose optional `icon_data_uri` metadata. The bridge discovers
artwork locally when creating or listing projects; no model call is involved.
The client uses the same artwork for project lists, navigation, project pickers,
search results, and workspace task rows. Missing or unreadable artwork falls back
to the first grapheme of the project name.

Discovery prefers `.omni-code/icon.*`, then conventional app icons and branding
assets such as `assets/app-icon.svg` and `assets/branding/mark.webp`, followed by
logo/favicon paths, web manifest icons, and standard Flutter platform icons.
Supported sources are SVG, PNG, WebP, ICO, JPEG, and GIF (first frame). To override
a discovered icon, add `.omni-code/icon.png` or `.omni-code/icon.svg` to the project.
Discovery is limited to known paths; it does not recursively scan dependencies.

Source files must stay within the project root, including symlink targets and
manifest references. Raster sources are limited to 2 MiB / 4096 pixels per axis,
decoded with an allocation limit, and resized to at most 96 × 96 PNG. SVG artwork
must be self-contained, without scripts, external resources, or embedded images.
Each encoded source is limited to 64 KiB before base64. Invalid candidates are
skipped so a later valid icon can still be selected.

Both successful and missing results are cached for 30 seconds in a bounded cache.
Adding, replacing, or removing artwork is reflected on a subsequent project list
or workspace snapshot refresh after this interval. Workspace snapshots include
one project identity record per relevant project; icon bytes are removed before
building the workspace assistant's model context. Older bridges and clients
remain compatible because all icon metadata is optional.
