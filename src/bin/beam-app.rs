use beam::indexer::{FileIndexHandle, IndexConfig, IndexStats, default_index_roots};
use gpui::{
    App, Bounds, Context, FontWeight, Render, Task, Window, WindowBounds, WindowOptions, div,
    prelude::*, px, rgb, size,
};
use gpui_platform::application;

struct BeamWindow {
    _controller: FileIndexHandle,
    stats: IndexStats,
    _bootstrap_task: Task<()>,
    _updates_task: Task<()>,
}

impl BeamWindow {
    fn new(cx: &mut Context<Self>) -> Self {
        let roots = default_index_roots();
        let controller = FileIndexHandle::spawn(
            IndexConfig::with_roots(roots.clone()),
            gpui_tokio::Tokio::handle(cx),
        )
        .expect("failed to start indexer");
        let stats = IndexStats::pending(roots);

        let controller_for_bootstrap = controller.clone();
        let bootstrap_task = cx.spawn(async move |this, cx| {
            if let Ok(snapshot) = controller_for_bootstrap.snapshot().await {
                update_window_stats(this, cx, snapshot.stats);
            }
        });

        let mut updates = controller.subscribe();
        let updates_task = cx.spawn(async move |this, cx| {
            while let Ok(update) = updates.recv().await {
                update_window_stats(this.clone(), cx, update.stats);
            }
        });

        Self {
            _controller: controller,
            stats,
            _bootstrap_task: bootstrap_task,
            _updates_task: updates_task,
        }
    }
}

impl Render for BeamWindow {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        let status_text = if self.stats.currently_scanning {
            "Scanning in the background"
        } else {
            "Indexer is idle"
        };

        div()
            .size_full()
            .bg(rgb(0xf2eadf))
            .text_color(rgb(0x1a1714))
            .child(
                div()
                    .size_full()
                    .p_6()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(div().text_sm().text_color(rgb(0x7b5c42)).child("BEAM / FILE INDEX"))
                                    .child(
                                        div()
                                            .text_2xl()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .child("Filesystem, without the stalls."),
                                    ),
                            )
                            .child(
                                div()
                                    .px_3()
                                    .py_2()
                                    .rounded_xl()
                                    .bg(rgb(0x1f3c35))
                                    .text_color(rgb(0xf8f4ed))
                                    .child(status_text),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .gap_3()
                            .flex_wrap()
                            .child(metric_card("Roots", self.stats.roots.len().to_string(), rgb(0xe2c48d).into()))
                            .child(metric_card("Files", self.stats.indexed_files.to_string(), rgb(0xb6d5c0).into()))
                            .child(metric_card(
                                "Directories",
                                self.stats.indexed_directories.to_string(),
                                rgb(0xb8cae5).into(),
                            ))
                            .child(metric_card(
                                "Symlinks",
                                self.stats.indexed_symlinks.to_string(),
                                rgb(0xd9b8dd).into(),
                            )),
                    )
                    .child(
                        div()
                            .rounded_xl()
                            .border_1()
                            .border_color(rgb(0xd2bfa7))
                            .bg(rgb(0xfcf8f2))
                            .p_4()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(div().text_sm().text_color(rgb(0x7b5c42)).child("Active roots"))
                            .child(div().child(join_roots(&self.stats)))
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(0x5c544b))
                                    .child(format!(
                                        "Last completed scan: {}",
                                        format_optional_timestamp(self.stats.last_scan_finished_at.as_ref()),
                                    )),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(0x5c544b))
                                    .child(format!(
                                        "Scan generation: {}",
                                        self.stats.scan_generation,
                                    )),
                            ),
                    )
                    .child(
                        div()
                            .rounded_xl()
                            .bg(rgb(0x1a1714))
                            .text_color(rgb(0xfaf6ef))
                            .p_4()
                            .child(
                                "The indexer runs on Tokio in the background, watches filesystem events, and keeps the UI thread clear for the launcher shell.",
                            ),
                    ),
            )
    }
}

fn main() {
    application().run(|cx: &mut App| {
        gpui_tokio::init(cx);

        let bounds = Bounds::centered(None, size(px(860.0), px(540.0)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |_, cx| cx.new(BeamWindow::new),
        )
        .expect("failed to open Beam window");

        cx.activate(true);
    });
}

fn metric_card(label: &str, value: String, tint: gpui::Hsla) -> impl IntoElement {
    div()
        .min_w(px(170.0))
        .flex()
        .flex_col()
        .gap_1()
        .p_4()
        .rounded_xl()
        .border_1()
        .border_color(rgb(0xd2bfa7))
        .bg(tint)
        .child(
            div()
                .text_sm()
                .text_color(rgb(0x4f463c))
                .child(label.to_string()),
        )
        .child(
            div()
                .text_xl()
                .font_weight(FontWeight::SEMIBOLD)
                .child(value),
        )
}

fn join_roots(stats: &IndexStats) -> String {
    stats
        .roots
        .iter()
        .map(|root| root.display().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_optional_timestamp(timestamp: Option<&chrono::DateTime<chrono::Utc>>) -> String {
    timestamp
        .map(|timestamp| timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "not yet completed".to_string())
}

fn update_window_stats(
    this: gpui::WeakEntity<BeamWindow>,
    cx: &mut gpui::AsyncApp,
    stats: IndexStats,
) {
    if let Some(this) = this.upgrade() {
        this.update(cx, |this, cx| {
            this.stats = stats;
            cx.notify();
        });
    }
}
