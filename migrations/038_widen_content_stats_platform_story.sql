-- 038_widen_content_stats_platform_story.sql
--
-- Allow platform = 'instagram_story' on content_stats.
--
-- Instagram stories are a genuinely different surface from feed posts: they live 24
-- hours, they carry a different metric vocabulary (taps forward/back, exits, swipes,
-- replies, profile visits — and NO comments, saves or permalink after expiry), and the
-- account publishes roughly an order of magnitude more of them. Writing them as
-- platform='instagram' would inject them into every existing query that filters on that
-- value — the competitor card's post selection, every average-engagement calculation,
-- the admin's unfiltered getAllContentStats() — and silently change what "our Instagram
-- performance" means on each of them. A separate platform value keeps the two apart,
-- exactly as youtube vs youtube_shorts already does for one channel.
--
-- WITHOUT THIS MIGRATION every story insert is rejected outright with a 23514 check
-- violation. That is a safe failure (nothing lands in the wrong bucket) but a total one
-- — the story capability simply does not work until this is applied.
--
-- NOT migrated here: the handful of pre-existing hand-entered story rows, which sit at
-- platform='instagram' with a NULL post_id and a calendar_item_id pointing at an
-- ig_story item. They cannot be matched to Zernio's story media ids — the account posts
-- many more story frames per day than it tracks as calendar deliverables, so matching on
-- date would attach an arbitrary frame's metrics to a tracked deliverable. Reconciling
-- them is an operator decision in the admin, not something to guess at here.
--
-- ⚠️ content_stats is shared with the Admin app (Curve_Admin_NextJS) — mirror this
-- change there if that repo ever re-creates the constraint.
--
-- Idempotent; applied manually like every other migration here.

alter table content_stats drop constraint if exists content_stats_platform_check;

alter table content_stats add constraint content_stats_platform_check
  check (
    platform is null
    or platform in (
      'instagram', 'tiktok', 'linkedin', 'youtube', 'youtube_shorts', 'instagram_story'
    )
  );
