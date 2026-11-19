//! Bounded, local-only project artwork discovery. Called on a blocking worker.
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::HashMap,
    fs::File,
    io::{Cursor, Read},
    path::Path,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

const MAX_FILE: u64 = 2 * 1024 * 1024;
const MAX_ICON: usize = 64 * 1024;
const CACHE_TTL: Duration = Duration::from_secs(30);
type Cache = HashMap<String, (Instant, Option<String>)>;
static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();

pub fn discover(root: &str) -> Option<String> {
    let cache = CACHE.get_or_init(Default::default);
    if let Some((at, icon)) = cache.lock().ok()?.get(root)
        && at.elapsed() < CACHE_TTL
    {
        return icon.clone();
    }
    let icon = Path::new(root)
        .canonicalize()
        .ok()
        .and_then(|root| discover_uncached(&root));
    if let Ok(mut cache) = cache.lock() {
        if cache.len() >= 256 {
            cache.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
        }
        if cache.len() >= 256
            && !cache.contains_key(root)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(root.to_owned(), (Instant::now(), icon.clone()));
    }
    icon
}

fn read_local(root: &Path, path: &Path, limit: u64) -> Option<Vec<u8>> {
    let path = path.canonicalize().ok()?;
    if !path.starts_with(root) {
        return None;
    }
    if !path.metadata().ok()?.is_file() {
        return None;
    }
    let file = File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > limit {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes).ok()?;
    (bytes.len() as u64 <= limit).then_some(bytes)
}

fn discover_uncached(root: &Path) -> Option<String> {
    let mut candidates = Vec::new();
    for stem in [
        ".omni-code/icon",
        "assets/app-icon",
        "assets/app_icon",
        "assets/icon",
        "assets/branding/mark",
        "assets/branding/logo",
        "assets/logo",
        "src/assets/logo",
        "app-icon",
        "icon",
        "logo",
        "public/logo",
        "public/icon",
        "static/logo",
        "static/icon",
        "src/app/icon",
        "app/icon",
        "src/app/favicon",
        "app/favicon",
        "public/favicon",
        "static/favicon",
        "favicon",
        "web/favicon",
    ] {
        for ext in ["svg", "png", "webp", "ico", "jpg", "jpeg", "gif"] {
            candidates.push(root.join(format!("{stem}.{ext}")));
        }
    }
    for manifest in [
        "public/manifest.json",
        "public/site.webmanifest",
        "web/manifest.json",
        "static/manifest.json",
        "manifest.json",
        "site.webmanifest",
    ] {
        let path = root.join(manifest);
        let Some(bytes) = read_local(root, &path, 64 * 1024) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            continue;
        };
        if let Some(icons) = value["icons"].as_array() {
            for icon in icons.iter().take(32) {
                if let Some(src) = icon["src"].as_str() {
                    // A leading slash in a web manifest is relative to its web root.
                    if src.contains(':') || src.contains('?') || src.contains('#') {
                        continue;
                    }
                    candidates.push(path.parent()?.join(src.trim_start_matches('/')));
                }
            }
        }
    }
    for path in [
        "web/icons/Icon-192.png",
        "macos/Runner/Assets.xcassets/AppIcon.appiconset/app_icon_128.png",
        "ios/Runner/Assets.xcassets/AppIcon.appiconset/Icon-App-60x60@2x.png",
        "android/app/src/main/res/mipmap-xxxhdpi/ic_launcher.png",
        "windows/runner/resources/app_icon.ico",
    ] {
        candidates.push(root.join(path));
    }
    candidates
        .into_iter()
        .find_map(|path| icon_data(root, &path))
}

fn icon_data(root: &Path, path: &Path) -> Option<String> {
    let bytes = read_local(root, path, MAX_FILE)?;
    if path.extension()?.eq_ignore_ascii_case("svg") {
        if bytes.len() > MAX_ICON {
            return None;
        }
        let text = std::str::from_utf8(&bytes).ok()?;
        let doc = roxmltree::Document::parse(text).ok()?;
        if doc.root_element().tag_name().name() != "svg" {
            return None;
        }
        // Only self-contained vector artwork; never fetch project-controlled URLs.
        for node in doc.descendants().filter(|n| n.is_element()) {
            if matches!(
                node.tag_name().name(),
                "script" | "foreignObject" | "image" | "style" | "animate" | "set"
            ) {
                return None;
            }
            for attr in node.attributes() {
                let value = attr.value().trim();
                if attr.name().starts_with("on")
                    || (attr.name() == "href" && !value.starts_with('#'))
                {
                    return None;
                }
                let lower = value.to_ascii_lowercase();
                if lower.contains("url(") {
                    let Some(reference) = lower
                        .strip_prefix("url(#")
                        .and_then(|s| s.strip_suffix(')'))
                    else {
                        return None;
                    };
                    if reference.is_empty()
                        || reference.chars().any(|c| {
                            c.is_whitespace() || matches!(c, '(' | ')' | '\'' | '"' | '\\')
                        })
                    {
                        return None;
                    }
                }
                if lower.contains("@import") {
                    return None;
                }
            }
        }
        return Some(format!(
            "data:image/svg+xml;base64,{}",
            STANDARD.encode(bytes)
        ));
    }
    let mut reader = image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(4096);
    limits.max_image_height = Some(4096);
    limits.max_alloc = Some(32 * 1024 * 1024);
    reader.limits(limits);
    let thumbnail = reader.decode().ok()?.thumbnail(96, 96);
    let mut encoded = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut encoded, image::ImageFormat::Png)
        .ok()?;
    let bytes = encoded.into_inner();
    (bytes.len() <= MAX_ICON).then(|| format!("data:image/png;base64,{}", STANDARD.encode(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    struct Project(PathBuf);
    impl Project {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("project-icon-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }
        fn write(&self, path: &str, bytes: &[u8]) {
            let path = self.0.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }
    impl Drop for Project {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    const SVG: &[u8] = br##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><path fill="#abc" d="M0 0h10v10z"/></svg>"##;
    #[test]
    fn branding_priority_and_invalid_fallback() {
        let p = Project::new();
        assert!(discover_uncached(&p.0).is_none());
        p.write("assets/app-icon.svg", SVG);
        p.write("public/favicon.svg", b"broken");
        assert_eq!(
            discover_uncached(&p.0),
            Some(format!(
                "data:image/svg+xml;base64,{}",
                STANDARD.encode(SVG)
            ))
        );
        p.write("assets/app-icon.svg", b"broken");
        p.write("assets/branding/mark.svg", SVG);
        assert!(discover_uncached(&p.0).is_some());
    }
    #[test]
    fn manifest_raster_is_resized_and_ico_supported() {
        let p = Project::new();
        p.write(
            "public/manifest.json",
            br#"{"icons":[{"src":"/brand.png"}]}"#,
        );
        image::DynamicImage::new_rgba8(512, 256)
            .save(p.0.join("public/brand.png"))
            .unwrap();
        let uri = discover_uncached(&p.0).unwrap();
        let image =
            image::load_from_memory(&STANDARD.decode(uri.split_once(',').unwrap().1).unwrap())
                .unwrap();
        assert_eq!((image.width(), image.height()), (96, 48));
        let mut ico = Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(32, 32)
            .write_to(&mut ico, image::ImageFormat::Ico)
            .unwrap();
        p.write("favicon.ico", &ico.into_inner());
        assert!(
            icon_data(&p.0, &p.0.join("favicon.ico"))
                .unwrap()
                .starts_with("data:image/png")
        );
    }
    #[test]
    fn unsafe_or_oversized_artwork_is_ignored() {
        let p = Project::new();
        for svg in [
            r#"<svg><image href="https://example.com/a.png"/></svg>"#,
            r#"<svg><script>alert(1)</script></svg>"#,
            r#"<svg><use href="../secret.svg"/></svg>"#,
        ] {
            p.write("logo.svg", svg.as_bytes());
            assert!(discover_uncached(&p.0).is_none());
        }
        p.write("logo.svg", &vec![b' '; MAX_ICON + 1]);
        assert!(discover_uncached(&p.0).is_none());
        p.write(
            "public/manifest.json",
            br#"{"icons":[{"src":"../../secret.svg"}]}"#,
        );
        assert!(discover_uncached(&p.0).is_none());
    }
    #[cfg(unix)]
    #[test]
    fn symlink_outside_project_is_ignored() {
        let p = Project::new();
        let outside = Project::new();
        outside.write("icon.svg", SVG);
        std::os::unix::fs::symlink(outside.0.join("icon.svg"), p.0.join("logo.svg")).unwrap();
        assert!(discover_uncached(&p.0).is_none());
    }
    #[test]
    fn expired_cache_detects_added_changed_and_removed_icon() {
        let p = Project::new();
        let key = p.0.to_str().unwrap();
        assert!(discover(key).is_none());
        p.write("logo.svg", SVG);
        let expire = || {
            CACHE.get().unwrap().lock().unwrap().get_mut(key).unwrap().0 =
                Instant::now() - CACHE_TTL;
        };
        expire();
        assert!(discover(key).is_some());
        p.write("logo.svg", b"broken");
        expire();
        assert!(discover(key).is_none());
        fs::remove_file(p.0.join("logo.svg")).unwrap();
        expire();
        assert!(discover(key).is_none());
    }
}
