mod dilate;

use anyhow::anyhow;
use assets_base::{
    verify::{ogg_vorbis::verify_ogg_vorbis, txt::verify_txt},
    AssetMetaEntryCommon, AssetUpload, AssetUploadResponse, AssetsIndex,
};
use image_utils::png::{
    is_png_image_valid, load_png_image_as_rgba, save_png_image, PngValidatorOptions,
};
use regex::Regex;
use reqwest::Url;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    num::{NonZero, NonZeroU64},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, MutexGuard};
use twilight_cache_inmemory::{DefaultInMemoryCache, ResourceType};
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::{request::channel::reaction::RequestReactionType, Client as HttpClient};
use twilight_mention::Mention;
use twilight_model::{
    application::interaction::InteractionData,
    channel::{
        message::{
            component::{ActionRow, Button, ButtonStyle},
            Component, EmojiReactionType, MessageFlags,
        },
        Attachment, Message,
    },
    gateway::payload::incoming::InteractionCreate,
    http::interaction::{InteractionResponse, InteractionResponseType},
    id::{
        marker::{
            ApplicationMarker, ChannelMarker, GuildMarker, MessageMarker, RoleMarker, UserMarker,
        },
        Id,
    },
};
use twilight_util::builder::{
    embed::{EmbedBuilder, EmbedFieldBuilder},
    InteractionResponseDataBuilder,
};
use twilight_validate::message::MESSAGE_CONTENT_LENGTH_MAX;

#[derive(Debug, Clone, Copy)]
struct ChannelAndRole {
    channel: Id<ChannelMarker>,
    role: Id<RoleMarker>,
}

#[derive(Debug)]
enum UploadAttachmentData {
    Png {
        data: Vec<u8>,
        width: NonZeroU64,
        height: NonZeroU64,
    },
    Ogg {
        data: Vec<u8>,
    },
    Txt {
        data: Vec<u8>,
    },
}

#[derive(Debug)]
struct UploadAssetMeta {
    name: String,
    authors: Vec<String>,
    licenses: Vec<String>,
    tags: Vec<String>,
    description: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum ReactionTy {
    ReactionVariant1,
    ReactionVariant2,
}

#[derive(Debug)]
struct UploadAttachments {
    data: Vec<UploadAttachmentData>,
    meta: UploadAssetMeta,
    /// Discord friendly author mention
    asset_author_mention: String,
    /// Discord friendly msg link
    msg_link: String,
    reaction_ty: ReactionTy,
    /// Will overwrite an existing asset
    will_overwrite: bool,
}

#[derive(Debug)]
enum UploadAttachmentMsg {
    Valid(UploadAttachments),
    Invalid(anyhow::Error),
}

#[derive(Debug)]
struct UploadingState {
    user: Id<UserMarker>,
    user_metion: String,
    channel_id: Id<ChannelMarker>,
    application_id: Id<ApplicationMarker>,
    interaction_token: String,

    attachments: HashMap<Id<MessageMarker>, UploadAttachmentMsg>,

    assets_index: AssetsIndex,

    /// must be reset with every user interaction.
    interaction_timeout: tokio::task::JoinHandle<()>,
}

impl UploadingState {
    fn errors(&self) -> Vec<String> {
        self.attachments
            .values()
            .filter_map(|a| {
                if let UploadAttachmentMsg::Invalid(err) = a {
                    Some(err.to_string())
                } else {
                    None
                }
            })
            .collect()
    }
    fn msg_long(&self) -> String {
        let errors: Vec<_> = self.errors();
        let mut has_warnings = false;
        let attachments: BTreeMap<_, _> = self.attachments.iter().map(|(k, v)| (*k, v)).collect();
        let valid: Vec<_> = attachments
            .values()
            .flat_map(|a| {
                if let UploadAttachmentMsg::Valid(a) = a {
                    if a.will_overwrite {
                        has_warnings = true;
                    }
                    a.data
                        .iter()
                        .map(|data| match data {
                            UploadAttachmentData::Png { width, height, .. } => {
                                format!(
                                    "[PNG] \"{}\" ({}x{}) - {}{}",
                                    a.meta.name,
                                    width,
                                    height,
                                    a.msg_link,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                            UploadAttachmentData::Ogg { data } => {
                                format!(
                                    "[OGG] \"{}\" ({} KiB) - {}{}",
                                    a.meta.name,
                                    data.len() / 1024,
                                    a.msg_link,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                            UploadAttachmentData::Txt { data } => {
                                format!(
                                    "[TXT] \"{}\" ({} KiB) - {}{}",
                                    a.meta.name,
                                    data.len() / 1024,
                                    a.msg_link,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                        })
                        .collect()
                } else {
                    Vec::default()
                }
            })
            .collect();
        format!(
            "
            {}\n\
            {}{}\
            __Uploading:__\n\
            {}\
            ",
            self.user_metion,
            if !errors.is_empty() {
                format!("__There are currently errors!__\n{}\n", errors.join("\n"))
            } else {
                "".to_string()
            },
            if has_warnings {
                "__⚠ Some assets will overwrite assets in the database ⚠__\n"
            } else {
                ""
            },
            valid.join("\n")
        )
    }

    fn msg_short(&self) -> String {
        let errors: Vec<_> = self.errors();
        let mut has_warnings = false;
        let attachments: BTreeMap<_, _> = self.attachments.iter().map(|(k, v)| (*k, v)).collect();
        let valid: Vec<_> = attachments
            .values()
            .flat_map(|a| {
                if let UploadAttachmentMsg::Valid(a) = a {
                    if a.will_overwrite {
                        has_warnings = true;
                    }
                    a.data
                        .iter()
                        .map(|data| match data {
                            UploadAttachmentData::Png { .. } => {
                                format!(
                                    "\"{}\"{}",
                                    a.meta.name,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                            UploadAttachmentData::Ogg { .. } => {
                                format!(
                                    "\"{}\"{}",
                                    a.meta.name,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                            UploadAttachmentData::Txt { .. } => {
                                format!(
                                    "\"{}\"{}",
                                    a.meta.name,
                                    if a.will_overwrite { " ⚠" } else { "" }
                                )
                            }
                        })
                        .collect()
                } else {
                    Vec::default()
                }
            })
            .collect();
        format!(
            "
            {}\n\
            {}{}\
            __Uploading:__\n\
            {}\
            ",
            self.user_metion,
            if !errors.is_empty() {
                format!("__There are currently errors!__\n{}\n", errors.join("\n"))
            } else {
                "".to_string()
            },
            if has_warnings {
                "__⚠ Some assets will overwrite assets in the database ⚠__\n"
            } else {
                ""
            },
            valid.join("\n")
        )
    }

    async fn update_msg(&self, http: Arc<HttpClient>) -> anyhow::Result<()> {
        let mut msg = self.msg_long();
        if msg.len() > MESSAGE_CONTENT_LENGTH_MAX {
            msg = self.msg_short();

            if msg.len() > MESSAGE_CONTENT_LENGTH_MAX {
                msg = msg
                    .char_indices()
                    .filter_map(|(byte_offset, c)| {
                        (byte_offset < MESSAGE_CONTENT_LENGTH_MAX - (32 + c.len_utf8()))
                            .then_some(c)
                    })
                    .collect();
                msg.push_str(" ...");
            }
        }
        http.interaction(self.application_id)
            .update_response(&self.interaction_token)
            .content(Some(&msg))
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
enum InteractionState {
    None,
    Uploading(UploadingState),
}

impl InteractionState {
    fn timeout(
        selfi: Arc<Mutex<InteractionState>>,
        http: Arc<HttpClient>,
        application_id: Id<ApplicationMarker>,
        token: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60 * 2)).await;

            let mut interaction = selfi.lock().await;
            *interaction = InteractionState::None;
            drop(interaction);

            let _ = http
                .interaction(application_id)
                .update_response(&token)
                .content(Some("Uploading timed out"))
                .embeds(None)
                .components(None)
                .await;
        })
    }
}

#[derive(Debug)]
struct Context {
    base_url: Url,
    upload_url: Url,
    guild_id: Id<GuildMarker>,

    map_resource_images: Option<ChannelAndRole>,
    map_resource_tilesets: Option<ChannelAndRole>,
    map_resource_sounds: Option<ChannelAndRole>,

    reaction_positive: RequestReactionType<'static>,
    reaction_negative: RequestReactionType<'static>,

    reaction_add_variant1: RequestReactionType<'static>,
    reaction_add_variant2: RequestReactionType<'static>,

    http: reqwest::Client,
    upload_password: String,

    interaction: Arc<Mutex<InteractionState>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    env_logger::init();

    let token = env::var("DISCORD_TOKEN")?;
    let guild_id = env::var("GUILD_ID")?;
    let upload_password = env::var("UPLOAD_PASSWORD")?;

    let map_resource_images_channel = env::var("MAP_RESOURCE_IMAGES_CHANNEL_ID").ok();
    let map_resource_images_role = env::var("MAP_RESOURCE_IMAGES_ROLE_ID").ok();
    let map_resource_tilesets_channel = env::var("MAP_RESOURCE_TILESETS_CHANNEL_ID").ok();
    let map_resource_tilesets_role = env::var("MAP_RESOURCE_TILESETS_ROLE_ID").ok();
    let map_resource_sounds_channel = env::var("MAP_RESOURCE_SOUNDS_CHANNEL_ID").ok();
    let map_resource_sounds_role = env::var("MAP_RESOURCE_SOUNDS_ROLE_ID").ok();

    let map_resource_images = map_resource_images_channel
        .map(|id| id.parse())
        .transpose()?
        .zip(map_resource_images_role.map(|id| id.parse()).transpose()?)
        .map(|(channel, role)| ChannelAndRole { channel, role });
    let map_resource_tilesets = map_resource_tilesets_channel
        .map(|id| id.parse())
        .transpose()?
        .zip(
            map_resource_tilesets_role
                .map(|id| id.parse())
                .transpose()?,
        )
        .map(|(channel, role)| ChannelAndRole { channel, role });
    let map_resource_sounds = map_resource_sounds_channel
        .map(|id| id.parse())
        .transpose()?
        .zip(map_resource_sounds_role.map(|id| id.parse()).transpose()?)
        .map(|(channel, role)| ChannelAndRole { channel, role });

    let base_url: Url = "https://pg.ddnet.org:7777/".try_into().unwrap();
    let context = Arc::new(Context {
        upload_url: base_url.join("upload/")?,
        base_url,
        guild_id: guild_id.parse()?,

        map_resource_images,
        map_resource_tilesets,
        map_resource_sounds,

        reaction_positive: RequestReactionType::Unicode { name: "👍" },
        reaction_negative: RequestReactionType::Unicode { name: "👎" },

        reaction_add_variant1: RequestReactionType::Unicode { name: "✅" },
        reaction_add_variant2: RequestReactionType::Unicode { name: "☑️" },

        http: reqwest::Client::new(),
        upload_password,

        interaction: Arc::new(Mutex::new(InteractionState::None)),
    });

    // Use intents to only receive guild message events.
    let mut shard = Shard::new(
        ShardId::ONE,
        token.clone(),
        Intents::GUILD_MESSAGES | Intents::GUILD_MESSAGE_REACTIONS | Intents::MESSAGE_CONTENT,
    );

    // HTTP is separate from the gateway, so create a new client.
    let http = Arc::new(HttpClient::new(token));

    let application_id = http.current_user_application().await?.model().await?.id;
    let interaction = http.interaction(application_id);
    interaction
        .create_guild_command(context.guild_id)
        .chat_input(
            "upload",
            "Upload an asset to the database, only works in the respective channel.",
        )
        .await?;
    interaction
        .create_guild_command(context.guild_id)
        .chat_input("upload_cancel", "Cancel the current upload process.")
        .await?;
    interaction
        .create_guild_command(context.guild_id)
        .chat_input("upload_finish", "Finish the current upload process.")
        .await?;

    // Since we only care about new messages, make the cache only
    // cache new messages.
    let cache = DefaultInMemoryCache::builder()
        .resource_types(ResourceType::MESSAGE)
        .build();

    // Process each event as they come in.
    while let Some(item) = shard.next_event(EventTypeFlags::all()).await {
        let event = match item {
            Ok(item) => item,
            Err(err) => {
                log::warn!("error receiving event: {err}");

                continue;
            }
        };

        if event.guild_id().is_none_or(|id| id != context.guild_id) {
            continue;
        }

        // Update the cache with the event.
        cache.update(&event);

        let context = context.clone();
        let http = Arc::clone(&http);
        tokio::spawn(async move {
            match handle_event(context, event, http).await {
                Ok(_) => {}
                Err(err) => {
                    log::error!("{err} - {}", err.backtrace());
                }
            }
        });
    }

    Ok(())
}

fn parse_msg(msg: &str) -> anyhow::Result<UploadAssetMeta> {
    /// Helper function to parse a list inside `[]` or a single item
    fn parse_list(input: &str) -> Vec<String> {
        let res = if input.starts_with('[') && input.ends_with(']') {
            input[1..input.len() - 1] // Remove brackets
                .split(',') // Split by commas
                .map(|s| s.trim().to_string()) // Trim spaces and convert to string
                .collect()
        } else if input.starts_with('(') && input.ends_with(')') {
            vec![input[1..input.len() - 1].trim().to_string()]
        } else {
            vec![input.trim().to_string()]
        };

        res.into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }
    let msg_regex = Regex::new(concat!(
        r#"^"(?P<asset_name>[^"]+)" "#,
        r#"by "#,
        r#"(?P<authors>\[?[^\]\[\(]+\]?) "#,
        r#"(?P<licenses>\[?\(?[^\]\[)]+\]?\)?)"#,
        r#"(?:\n(?P<tags>\[[^\]\[]+\])\n?"#,
        r#"(?P<description>.*)?)?"#,
    ))?;
    let caps = msg_regex.captures(msg).ok_or_else(|| {
        anyhow!(
            "Invalid or missing meta data. Try `\"asset_name\" by author (license)` \
            and optionally you can add tags `[tag1,tag2]` after a new line and \
            a description after another new line."
        )
    })?;

    let res = UploadAssetMeta {
        name: caps
            .name("asset_name")
            .ok_or_else(|| anyhow!("Asset name not found"))?
            .as_str()
            .trim()
            .to_string(),
        authors: parse_list(
            caps.name("authors")
                .ok_or_else(|| anyhow!("Authors not found"))?
                .as_str(),
        ),
        licenses: parse_list(
            caps.name("licenses")
                .ok_or_else(|| anyhow!("Licenses not found"))?
                .as_str(),
        ),
        tags: caps
            .name("tags")
            .map(|tags| parse_list(tags.as_str()))
            .unwrap_or_default(),
        description: caps.name("description").map(|d| d.as_str().to_string()),
    };

    let allowed_licenses = ["CC0", "CC BY", "CC BY-SA"];
    if !res
        .licenses
        .iter()
        .all(|l| allowed_licenses.contains(&l.as_str()))
    {
        anyhow::bail!(
            "Licenses must be either of: {}",
            allowed_licenses.join(", ")
        );
    }

    if res.authors.is_empty()
        || res.licenses.is_empty()
        || res.authors.iter().any(|a| a.is_empty())
        || res.licenses.iter().any(|a| a.is_empty())
        || res.tags.iter().any(|a| a.is_empty())
    {
        anyhow::bail!(
            "List of authors and licenses must be non empty. Every author, \
            license and tag is not allowed to be empty."
        );
    }

    Ok(res)
}

async fn attachment_per_channel(
    context: &Context,
    channel: Id<ChannelMarker>,
    attachment: &Attachment,
    attachment_index: usize,
) -> anyhow::Result<UploadAttachmentData> {
    let ty = attachment
        .content_type
        .as_ref()
        .ok_or_else(|| anyhow!("unknown attachment type"))?;

    let mut allows_img = None;
    let mut allows_snd = None;
    let mut max_attachments = 0;
    let mut dilate_tileset = false;
    if context
        .map_resource_images
        .is_some_and(|c| c.channel == channel)
    {
        allows_img = Some(PngValidatorOptions {
            max_width: NonZero::new(2048).unwrap(),
            max_height: NonZero::new(2048).unwrap(),
            min_width: None,
            min_height: None,
            divisible_width: None,
            divisible_height: None,
        });
        max_attachments = 1;
    } else if context
        .map_resource_tilesets
        .is_some_and(|c| c.channel == channel)
    {
        allows_img = Some(PngValidatorOptions {
            max_width: NonZero::new(1024).unwrap(),
            max_height: NonZero::new(1024).unwrap(),
            min_width: Some(NonZero::new(1024).unwrap()),
            min_height: Some(NonZero::new(1024).unwrap()),
            divisible_width: None,
            divisible_height: None,
        });
        max_attachments = 1;
        dilate_tileset = true;
    } else if context
        .map_resource_sounds
        .is_some_and(|c| c.channel == channel)
    {
        allows_snd = Some(1024 * 1024);
        max_attachments = 1;
    } else {
        // ignore unknown channel
    }

    let file = context
        .http
        .get(&attachment.url)
        .send()
        .await?
        .bytes()
        .await?
        .to_vec();

    anyhow::ensure!(
        attachment_index < max_attachments,
        "Too many attachments found, max is {}",
        max_attachments
    );

    Ok(match ty.as_str() {
        "image/png" => {
            let img_limits =
                allows_img.ok_or_else(|| anyhow!("Images are not allowed in this channel"))?;

            is_png_image_valid(&file, img_limits)?;

            let mut mem: Vec<u8> = Default::default();
            let img = load_png_image_as_rgba(&file, |width, height, _| {
                mem.resize(width * height * 4, 0);
                &mut mem
            })?;

            let w = img.width as usize;
            let h = img.height as usize;
            if dilate_tileset {
                let sw = w / 16;
                let sh = h / 16;
                for x in 0..16 {
                    for y in 0..16 {
                        dilate::dilate_image_sub(&mut mem, w, h, 4, x * sw, y * sh, sw, sh);
                    }
                }
            } else {
                dilate::dilate_image(&mut mem, w, h, 4);
            }
            let data = save_png_image(&mem, w as u32, h as u32)?;

            UploadAttachmentData::Png {
                data,
                width: NonZeroU64::new(
                    attachment
                        .width
                        .ok_or_else(|| anyhow!("Attachment width was None"))?,
                )
                .ok_or_else(|| anyhow!("Attachment width was 0"))?,
                height: NonZeroU64::new(
                    attachment
                        .height
                        .ok_or_else(|| anyhow!("Attachment height was None"))?,
                )
                .ok_or_else(|| anyhow!("Attachment height was 0"))?,
            }
        }
        "audio/ogg" => {
            let snd_limits =
                allows_snd.ok_or_else(|| anyhow!("Sounds are not allowed in this channel"))?;
            anyhow::ensure!(
                file.len() <= snd_limits,
                "Sounds are limited to a size of {} KiB",
                snd_limits / 1024
            );
            verify_ogg_vorbis(&file)?;
            UploadAttachmentData::Ogg { data: file }
        }
        "text/plain" => {
            verify_txt(&file, &attachment.filename)?;
            UploadAttachmentData::Txt { data: file }
        }
        ty => {
            anyhow::bail!("Unsupported content type: {ty}");
        }
    })
}

async fn upload_attachment_per_channel(
    context: &Context,
    attachments: UploadAttachments,
    channel_id: Id<ChannelMarker>,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let (upload_url_img, upload_url_sound, upload_url_txt, category) = if context
        .map_resource_images
        .is_some_and(|c| c.channel == channel_id)
    {
        Some(("map/resources/images", "", "", Some("img".to_string())))
    } else if context
        .map_resource_tilesets
        .is_some_and(|c| c.channel == channel_id)
    {
        Some(("map/resources/images", "", "", Some("tileset".to_string())))
    } else if context
        .map_resource_sounds
        .is_some_and(|c| c.channel == channel_id)
    {
        Some(("", "map/resources/sounds", "", None))
    } else {
        None
    }
    .ok_or_else(|| anyhow!("Invalid attachment for this channel"))?;

    let mut succeeded_uploads = Vec::default();
    let mut failed_uploads = Vec::default();
    for (index, attachment) in attachments.data.into_iter().enumerate() {
        let (upload_url, data, extension) = match attachment {
            UploadAttachmentData::Png { data, .. } => (upload_url_img, data, "png"),
            UploadAttachmentData::Ogg { data } => (upload_url_sound, data, "ogg"),
            UploadAttachmentData::Txt { data } => (upload_url_txt, data, "txt"),
        };

        if upload_url.is_empty() {
            failed_uploads.push(format!(
                "Failed to upload attachment {} of {}",
                index, attachments.meta.name
            ));
            continue;
        }

        let res_raw = context
            .http
            .post(context.upload_url.join(upload_url)?)
            .body(serde_json::to_vec(&AssetUpload {
                upload_password: context.upload_password.clone(),
                meta: AssetMetaEntryCommon {
                    authors: attachments.meta.authors.clone(),
                    licenses: attachments.meta.licenses.clone(),
                    tags: attachments.meta.tags.clone(),
                    category: category.clone(),
                    description: attachments.meta.description.clone().unwrap_or_default(),
                    release_date_utc: chrono::Utc::now(),
                    extra: Default::default(),
                },
                name: attachments.meta.name.clone(),
                extension: extension.to_string(),
                data,
            })?)
            .header("content-type", "application/json")
            .send()
            .await?
            .bytes()
            .await?;
        let res: AssetUploadResponse = serde_json::from_slice(&res_raw)?;

        match res {
            AssetUploadResponse::Success => {
                succeeded_uploads.push(format!(
                    "\"{}\" by {} ({})",
                    attachments.meta.name, attachments.asset_author_mention, attachments.msg_link
                ));
            }
            AssetUploadResponse::IncorrectPassword => {
                failed_uploads.push("incorrect password".to_string());
            }
            AssetUploadResponse::IncompleteMetadata => {
                failed_uploads.push("incomplete metadata".to_string());
            }
            AssetUploadResponse::InvalidCategory => {
                failed_uploads.push("invalid category".to_string());
            }
            AssetUploadResponse::InvalidName => {
                failed_uploads.push("invalid name".to_string());
            }
            AssetUploadResponse::UnsupportedFileType => {
                failed_uploads.push("unsupported file type".to_string());
            }
            AssetUploadResponse::BrokenFile(err) => {
                failed_uploads.push(err);
            }
            AssetUploadResponse::WritingFailed => {
                failed_uploads.push("writing file failed".to_string());
            }
        }
    }
    Ok((succeeded_uploads, failed_uploads))
}

fn explain_upload_per_channel_reaction(
    context: &Context,
    channel_id: Id<ChannelMarker>,
    reaction_ty: ReactionTy,
) -> Option<String> {
    if context
        .map_resource_images
        .is_some_and(|c| c.channel == channel_id)
    {
        match reaction_ty {
            ReactionTy::ReactionVariant1 => Some("quad image".to_string()),
            ReactionTy::ReactionVariant2 => None,
        }
    } else if context
        .map_resource_tilesets
        .is_some_and(|c| c.channel == channel_id)
    {
        match reaction_ty {
            ReactionTy::ReactionVariant1 => Some("tile set".to_string()),
            ReactionTy::ReactionVariant2 => None,
        }
    } else if context
        .map_resource_sounds
        .is_some_and(|c| c.channel == channel_id)
    {
        match reaction_ty {
            ReactionTy::ReactionVariant1 => Some("sound".to_string()),
            ReactionTy::ReactionVariant2 => None,
        }
    } else {
        None
    }
}

async fn finish_upload(
    context: &Context,
    http: &HttpClient,
    mut interaction: MutexGuard<'_, InteractionState>,
    author_id: Id<UserMarker>,
    channel_id: Id<ChannelMarker>,
) -> anyhow::Result<()> {
    if let InteractionState::Uploading(state) = &mut *interaction {
        if state.user != author_id {
            // simply ignore
            return Ok(());
        }
        if state.channel_id != channel_id {
            // simply ignore
            return Ok(());
        }

        let mut succeeded_attachments = Vec::new();
        let mut failed_attachments = Vec::new();
        for (_, attachment) in state.attachments.drain() {
            match attachment {
                UploadAttachmentMsg::Valid(upload_attachments) => {
                    let (succeeded_uploads, failed_uploads) =
                        upload_attachment_per_channel(context, upload_attachments, channel_id)
                            .await?;
                    failed_attachments.extend(failed_uploads);
                    succeeded_attachments.extend(succeeded_uploads);
                }
                UploadAttachmentMsg::Invalid(error) => {
                    failed_attachments.push(error.to_string());
                }
            }
        }

        let application_id = state.application_id;
        let token = state.interaction_token.clone();
        let channel_id = state.channel_id;
        *interaction = InteractionState::None;
        drop(interaction);
        if !failed_attachments.is_empty() {
            let mut msg = format!("Some uploads failed:\n{}", failed_attachments.join("\n"));

            if msg.len() > MESSAGE_CONTENT_LENGTH_MAX {
                msg = msg
                    .char_indices()
                    .filter_map(|(byte_offset, c)| {
                        (byte_offset < MESSAGE_CONTENT_LENGTH_MAX - (32 + c.len_utf8()))
                            .then_some(c)
                    })
                    .collect();
                msg.push_str(" ...");
            }
            http.interaction(application_id)
                .update_response(&token)
                .content(Some(&msg))
                .await?;
        } else {
            http.interaction(application_id)
                .delete_response(&token)
                .await?;
        }

        if !succeeded_attachments.is_empty() {
            let mut msgs: Vec<String> = Default::default();
            msgs.push("The following assets were upload to the database:\n".to_string());

            for succeeded_upload in succeeded_attachments {
                let msg = msgs.last_mut().unwrap();
                let add_str = format!("- {}\n", succeeded_upload);
                if msg.len() + succeeded_upload.len() <= MESSAGE_CONTENT_LENGTH_MAX {
                    msg.push_str(&add_str);
                } else {
                    msgs.push(add_str);
                }
            }

            for msg in msgs {
                http.create_message(channel_id).content(&msg).await?;
            }
        }
    }
    Ok(())
}

async fn cancel_upload(
    http: &HttpClient,
    mut interaction: MutexGuard<'_, InteractionState>,
    author_id: Id<UserMarker>,
    channel_id: Id<ChannelMarker>,
) -> anyhow::Result<()> {
    if let InteractionState::Uploading(state) = &mut *interaction {
        if state.user != author_id {
            // simply ignore
            return Ok(());
        }
        if state.channel_id != channel_id {
            // simply ignore
            return Ok(());
        }

        let application_id = state.application_id;

        let token = state.interaction_token.clone();
        *interaction = InteractionState::None;
        drop(interaction);

        http.interaction(application_id)
            .delete_response(&token)
            .await?;
    }
    Ok(())
}

async fn handle_interaction(
    context: Arc<Context>,
    http: Arc<HttpClient>,
    ev: &InteractionCreate,
) -> anyhow::Result<()> {
    let reply = |msg: String| async {
        http.interaction(ev.application_id)
            .create_response(
                ev.id,
                &ev.token,
                &InteractionResponse {
                    kind: InteractionResponseType::ChannelMessageWithSource,
                    data: Some(
                        InteractionResponseDataBuilder::new()
                            .content(msg)
                            .flags(MessageFlags::EPHEMERAL)
                            .build(),
                    ),
                },
            )
            .await?;
        anyhow::Ok(())
    };

    match (&ev.data, &ev.channel, ev.author()) {
        (Some(InteractionData::ApplicationCommand(data)), Some(channel), Some(author)) => {
            let roles = http
                .guild_member(context.guild_id, author.id)
                .await?
                .model()
                .await?
                .roles;

            // check if the channel is allowed and author has the rights
            if !context
                .map_resource_images
                .into_iter()
                .chain(context.map_resource_tilesets.into_iter())
                .chain(context.map_resource_sounds.into_iter())
                .any(|c| c.channel == channel.id && roles.contains(&c.role))
            {
                reply("Insufficient permissions".to_string()).await?;
                return Ok(());
            }

            let interaction_task = context.interaction.clone();
            let mut interaction = context.interaction.lock().await;

            match &*interaction {
                InteractionState::None => {}
                InteractionState::Uploading(state) => {
                    if state.user != author.id {
                        reply("Another user is currently using the asset commands".to_string())
                            .await?;
                        return Ok(());
                    }
                }
            }

            match data.name.as_str() {
                "upload" => {
                    if !matches!(*interaction, InteractionState::None) {
                        reply("An upload is already in progress".to_string()).await?;
                        return Ok(());
                    }

                    let start_msg = format!(
                        "\
                    {}\n\
                    __**:art: You are about to upload assets to the database.**__\n\n\
                    ",
                        author.mention()
                    );
                    let embed_react = EmbedBuilder::new()
                        .color(0x1ABC9C)
                        .field(EmbedFieldBuilder::new(
                            "Please react to all assets you want to upload:",
                            explain_upload_per_channel_reaction(
                                &context,
                                channel.id,
                                ReactionTy::ReactionVariant1,
                            )
                            .map(|s| {
                                format!(
                                    "- React with {} to upload an asset to the {} database",
                                    match &context.reaction_add_variant1 {
                                        RequestReactionType::Unicode { name } => {
                                            name.to_string()
                                        }
                                        RequestReactionType::Custom { name, id } => {
                                            format!("<:{}:{}>", name.unwrap_or_default(), id)
                                        }
                                    },
                                    s
                                )
                            })
                            .into_iter()
                            .chain(
                                explain_upload_per_channel_reaction(
                                    &context,
                                    channel.id,
                                    ReactionTy::ReactionVariant2,
                                )
                                .map(|s| {
                                    format!(
                                        "- React with {} to upload an asset to the {} database",
                                        match &context.reaction_add_variant2 {
                                            RequestReactionType::Unicode { name } => {
                                                name.to_string()
                                            }
                                            RequestReactionType::Custom { name, id } => {
                                                format!("<:{}:{}>", name.unwrap_or_default(), id)
                                            }
                                        },
                                        s
                                    )
                                })
                                .into_iter(),
                            )
                            .collect::<Vec<_>>()
                            .join("\n"),
                        ))
                        .build();
                    let embed_finish = EmbedBuilder::new()
                        .color(0xE67E22)
                        .field(EmbedFieldBuilder::new(
                            "",
                            "\
                        Once you are done, use the 🆗 button or the command `/upload_finish`\n\
                        To cancel the upload, use the 🇽 button or the command `/upload_cancel`\n",
                        ))
                        .build();
                    struct Btn<'a> {
                        id: &'a str,
                        emoji: &'a str,
                    }
                    fn buttons(btns: Vec<Btn>) -> Component {
                        Component::ActionRow(ActionRow {
                            components: btns
                                .into_iter()
                                .map(|Btn { id, emoji }| {
                                    Component::Button(Button {
                                        custom_id: Some(id.to_string()),
                                        disabled: false,
                                        emoji: Some(EmojiReactionType::Unicode {
                                            name: emoji.to_string(),
                                        }),
                                        label: None,
                                        style: ButtonStyle::Primary,
                                        url: None,
                                        sku_id: None,
                                    })
                                })
                                .collect(),
                        })
                    }
                    let btn_ok_cancel = buttons(vec![
                        Btn {
                            id: "ok",
                            emoji: "🆗",
                        },
                        Btn {
                            id: "cancel",
                            emoji: "🇽",
                        },
                    ]);
                    http.interaction(ev.application_id)
                        .create_response(
                            ev.id,
                            &ev.token,
                            &InteractionResponse {
                                kind: InteractionResponseType::ChannelMessageWithSource,
                                data: Some(
                                    InteractionResponseDataBuilder::new()
                                        .content(start_msg)
                                        .flags(MessageFlags::EPHEMERAL)
                                        .embeds([embed_react, embed_finish])
                                        .components([btn_ok_cancel])
                                        .build(),
                                ),
                            },
                        )
                        .await?;

                    let index_url = if context
                        .map_resource_images
                        .is_some_and(|c| c.channel == channel.id)
                        || context
                            .map_resource_tilesets
                            .is_some_and(|c| c.channel == channel.id)
                    {
                        Some("map/resources/images/")
                    } else if context
                        .map_resource_sounds
                        .is_some_and(|c| c.channel == channel.id)
                    {
                        Some("map/resources/sounds/")
                    } else {
                        None
                    };
                    *interaction = InteractionState::Uploading(UploadingState {
                        user: author.id,
                        user_metion: author.mention().to_string(),
                        channel_id: channel.id,
                        application_id: ev.application_id,
                        interaction_token: ev.token.clone(),
                        attachments: Default::default(),

                        interaction_timeout: InteractionState::timeout(
                            interaction_task,
                            http,
                            ev.application_id,
                            ev.token.clone(),
                        ),

                        assets_index: serde_json::from_slice(
                            &context
                                .http
                                .get(
                                    context
                                        .base_url
                                        .join(index_url.ok_or_else(|| {
                                            anyhow!("Channel had no asset index url")
                                        })?)?
                                        .join("index.json")?,
                                )
                                .send()
                                .await?
                                .bytes()
                                .await?,
                        )?,
                    });
                }
                "upload_cancel" => {
                    cancel_upload(&http, interaction, author.id, channel.id).await?;
                }
                "upload_finish" => {
                    finish_upload(&context, &http, interaction, author.id, channel.id).await?;
                }
                _ => {
                    log::debug!("ignored command: {}", data.name);
                }
            }
        }
        (Some(InteractionData::MessageComponent(data)), Some(channel), Some(author)) => {
            let interaction = context.interaction.lock().await;

            match data.custom_id.as_str() {
                "ok" => {
                    finish_upload(&context, &http, interaction, author.id, channel.id).await?;
                }
                "cancel" => {
                    cancel_upload(&http, interaction, author.id, channel.id).await?;
                }
                custom_id => {
                    log::debug!("ignored unknown message component id: {custom_id}");
                }
            }
        }
        _ => {
            log::debug!("Unhandled interaction: {:?}", ev);
        }
    }

    Ok(())
}

fn in_channel(context: &Context, channel: Id<ChannelMarker>) -> bool {
    context
        .map_resource_images
        .map(|c| c.channel)
        .into_iter()
        .chain(context.map_resource_tilesets.map(|c| c.channel))
        .chain(context.map_resource_sounds.map(|c| c.channel))
        .any(|c| c == channel)
}

async fn handle_event(
    context: Arc<Context>,
    event: Event,
    http: Arc<HttpClient>,
) -> anyhow::Result<()> {
    fn check_emoji(expected: &RequestReactionType<'static>, val: &EmojiReactionType) -> bool {
        // ignores emojis that are not important for uploading
        match (expected, val) {
            (
                RequestReactionType::Custom {
                    id: id1,
                    name: name1,
                },
                EmojiReactionType::Custom {
                    id: id2,
                    name: name2,
                    ..
                },
            ) => id1 == id2 && *name1 == name2.as_ref().map(|s| s.as_str()),
            (RequestReactionType::Custom { .. }, EmojiReactionType::Unicode { .. })
            | (RequestReactionType::Unicode { .. }, EmojiReactionType::Custom { .. }) => false,
            (
                RequestReactionType::Unicode { name: name1 },
                EmojiReactionType::Unicode { name: name2 },
            ) => name1 == name2,
        }
    }
    async fn delete_and_pm(
        http: &HttpClient,
        msg: &Message,
        err: anyhow::Error,
    ) -> anyhow::Result<()> {
        http.delete_message(msg.channel_id, msg.id).await?;

        let dm = http
            .create_private_channel(msg.author.id)
            .await?
            .model()
            .await?;
        let mut msg = err.to_string();
        if msg.len() > MESSAGE_CONTENT_LENGTH_MAX {
            msg = msg
                .char_indices()
                .filter_map(|(byte_offset, c)| {
                    (byte_offset < MESSAGE_CONTENT_LENGTH_MAX - (32 + c.len_utf8())).then_some(c)
                })
                .collect();
            msg.push_str(" ...");
        }
        http.create_message(dm.id).content(&msg).await?;
        anyhow::Ok(())
    }

    match event {
        Event::MessageCreate(msg) => {
            if msg.author.bot {
                return Ok(());
            }
            // ignore msgs from invalid/other channels
            if !in_channel(&context, msg.channel_id) {
                return Ok(());
            }
            // meta data not interesting, only if it works or not
            match parse_msg(&msg.content) {
                Ok(_) => {}
                Err(err) => {
                    delete_and_pm(&http, &msg, err).await?;
                    return Ok(());
                }
            }
            let mut it = msg.attachments.iter().enumerate();
            let a = loop {
                let Some((index, attachment)) = it.next() else {
                    break Ok(());
                };
                match attachment_per_channel(&context, msg.channel_id, attachment, index).await {
                    Ok(_) => {}
                    Err(err) => break Err(err),
                }
            };
            if let Err(err) = a.and(
                (!msg.attachments.is_empty())
                    .then_some(())
                    .ok_or_else(|| anyhow!("Your message is missing an asset attachment")),
            ) {
                delete_and_pm(&http, &msg, err).await?;
                return Ok(());
            }

            http.create_reaction(msg.channel_id, msg.id, &context.reaction_positive)
                .await?;
            http.create_reaction(msg.channel_id, msg.id, &context.reaction_negative)
                .await?;
        }
        Event::MessageUpdate(ev) => {
            // ignore ev from invalid/other channels
            if !in_channel(&context, ev.channel_id) {
                return Ok(());
            }

            delete_and_pm(&http, &ev, anyhow!("Updating messages is not supported. Simply delete your old message and create a new one.")).await?;
        }
        Event::InteractionCreate(ev) => handle_interaction(context, http, &ev).await?,
        Event::ReactionAdd(ev) => {
            // ignore ev from invalid/other channels
            if !in_channel(&context, ev.channel_id) {
                return Ok(());
            }
            let react1 = check_emoji(&context.reaction_add_variant1, &ev.emoji);
            let react2 = check_emoji(&context.reaction_add_variant2, &ev.emoji);
            if !react1 && !react2 {
                return Ok(());
            }
            let reaction_ty = if react1 {
                ReactionTy::ReactionVariant1
            } else {
                ReactionTy::ReactionVariant2
            };

            let interaction_task = context.interaction.clone();
            let mut interaction = context.interaction.lock().await;
            if let InteractionState::Uploading(state) = &mut *interaction {
                if state.user != ev.user_id {
                    // simply ignore
                    return Ok(());
                }
                if state.channel_id != ev.channel_id {
                    // simply ignore
                    return Ok(());
                }
                state.interaction_timeout.abort();
                state.interaction_timeout = InteractionState::timeout(
                    interaction_task,
                    http.clone(),
                    state.application_id,
                    state.interaction_token.clone(),
                );

                // check if the asset should be uploaded
                let msg = http
                    .message(ev.channel_id, ev.message_id)
                    .await?
                    .model()
                    .await?;

                // take the moment to remove other reactions
                for reaction in msg.reactions.iter() {
                    if react1 && check_emoji(&context.reaction_add_variant2, &reaction.emoji) {
                        let _ = http
                            .delete_all_reaction(
                                ev.channel_id,
                                ev.message_id,
                                &context.reaction_add_variant2,
                            )
                            .await;
                    } else if react2 && check_emoji(&context.reaction_add_variant1, &reaction.emoji)
                    {
                        let _ = http
                            .delete_all_reaction(
                                ev.channel_id,
                                ev.message_id,
                                &context.reaction_add_variant1,
                            )
                            .await;
                    }
                }
                match parse_msg(&msg.content) {
                    Ok(meta) => {
                        let attachments =
                            state.attachments.entry(ev.message_id).or_insert_with(|| {
                                UploadAttachmentMsg::Valid(UploadAttachments {
                                    data: Default::default(),
                                    meta,
                                    asset_author_mention: msg.author.mention().to_string(),
                                    msg_link: format!(
                                        "https://discord.com/channels/{}/{}/{}",
                                        context.guild_id, state.channel_id, msg.id
                                    ),
                                    reaction_ty,
                                    will_overwrite: false,
                                })
                            });
                        if let UploadAttachmentMsg::Valid(upload_attachments) = attachments {
                            upload_attachments.data.clear();
                            upload_attachments.reaction_ty = reaction_ty;
                        }
                        if msg.attachments.is_empty() {
                            *attachments = UploadAttachmentMsg::Invalid(anyhow!(
                                "No attachments found for msg"
                            ));
                        }

                        for (index, attachment) in msg.attachments.iter().enumerate() {
                            match attachment_per_channel(&context, ev.channel_id, attachment, index)
                                .await
                            {
                                Ok(res) => {
                                    if let UploadAttachmentMsg::Valid(upload_attachments) =
                                        attachments
                                    {
                                        let in_index = || match res {
                                            UploadAttachmentData::Png { .. } => Ok(state
                                                .assets_index
                                                .contains_key(&upload_attachments.meta.name)),
                                            UploadAttachmentData::Ogg { .. } => Ok(state
                                                .assets_index
                                                .contains_key(&upload_attachments.meta.name)),
                                            UploadAttachmentData::Txt { .. } => Ok(state
                                                .assets_index
                                                .contains_key(&upload_attachments.meta.name)),
                                        };
                                        match in_index() {
                                            Ok(in_index) => {
                                                upload_attachments.data.push(res);
                                                upload_attachments.will_overwrite |= in_index;
                                            }
                                            Err(err) => {
                                                *attachments = UploadAttachmentMsg::Invalid(err);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    *attachments = UploadAttachmentMsg::Invalid(err);
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let attachments =
                            state.attachments.entry(ev.message_id).or_insert_with(|| {
                                UploadAttachmentMsg::Invalid(anyhow!("created invalidly."))
                            });
                        *attachments = UploadAttachmentMsg::Invalid(err);
                    }
                }
                state.update_msg(http).await?;
            }
        }
        Event::ReactionRemove(ev) => {
            // ignore ev from invalid/other channels
            if !in_channel(&context, ev.channel_id) {
                return Ok(());
            }
            let react1 = check_emoji(&context.reaction_add_variant1, &ev.emoji);
            let react2 = check_emoji(&context.reaction_add_variant2, &ev.emoji);
            if !react1 && !react2 {
                return Ok(());
            }

            let interaction_task = context.interaction.clone();
            let mut interaction = context.interaction.lock().await;
            if let InteractionState::Uploading(state) = &mut *interaction {
                if state.user != ev.user_id {
                    // simply ignore
                    return Ok(());
                }
                if state.channel_id != ev.channel_id {
                    // simply ignore
                    return Ok(());
                }
                state.interaction_timeout.abort();
                state.interaction_timeout = InteractionState::timeout(
                    interaction_task,
                    http.clone(),
                    state.application_id,
                    state.interaction_token.clone(),
                );

                // remove msg again
                state.attachments.remove(&ev.message_id);

                state.update_msg(http).await?;
            }
        }
        // Other events here...
        e => {
            log::debug!("ignored event: {:?}", e);
        }
    }

    Ok(())
}
