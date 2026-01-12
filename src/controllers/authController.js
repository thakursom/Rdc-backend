const axios = require("axios");
const crypto = require("crypto");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");

const ResponseService = require("../services/responseService");
const sendEmail = require("../utils/sendEmail");
const User = require("../models/userModel");
const Artist = require("../models/artistModel");
const LogService = require("../services/logService");
const Release = require("../models/releaseModel");
const Track = require("../models/trackModel");

class AuthController {

    constructor() { }

    //login method
    async login(req, res) {
        try {
            const { email, password } = req.body;

            const user = await User.findOne({ email });
            if (!user) {

                await LogService.createLog({
                    email,
                    action: "LOGIN_FAILED",
                    description: `Login failed: user not found (${email})`,
                    req
                });

                return ResponseService.error(res, "User not found", 404);
            }

            const match = await bcrypt.compare(password, user.password);
            if (!match) {

                await LogService.createLog({
                    user_id: user.id,
                    email: user.email,
                    action: "LOGIN_FAILED",
                    description: `Login failed: invalid password (${email})`,
                    req
                });

                return ResponseService.error(res, "Invalid password", 400);
            }

            const token = jwt.sign(
                {
                    _id: user._id,
                    userId: user.id,
                    name: user.name,
                    email: user.email,
                    role: user.role
                },
                process.env.JWT_SECRET,
                { expiresIn: "1d" }
            );

            await LogService.createLog({
                user_id: user.id,
                email: user.email,
                action: "LOGIN_SUCCESS",
                description: `${user.email} logged in successfully`,
                req
            });

            return ResponseService.success(res, "Login successful", {
                token,
                user: {
                    name: user.name,
                    email: user.email,
                    role: user.role,
                    userId: user.id
                }
            });

        } catch (error) {
            // await LogService.createLog({
            //     email: req.body.email,
            //     action: "LOGIN_ERROR",
            //     description: "Unexpected login error",
            //     req
            // });

            return ResponseService.error(res, "Login failed", 500, error);
        }
    }

    //forgotPassword method
    async forgotPassword(req, res) {
        try {
            const { email } = req.body;

            const user = await User.findOne({ email });
            if (!user) return ResponseService.error(res, "User not found", 404);

            // create secure token
            const resetToken = crypto.randomBytes(32).toString("hex");
            const hashedToken = crypto.createHash("sha256").update(resetToken).digest("hex");

            user.resetToken = hashedToken;
            user.resetTokenExpire = Date.now() + 15 * 60 * 1000; // 15 min

            await user.save();

            // You will email this link in real apps
            const resetUrl = `${process.env.FRONTEND_URL}/reset-password/${resetToken}`;
            console.log(resetUrl);


            // Send Email
            await sendEmail(
                email,
                "Password Reset Request",
                `
                <h3>Password Reset Requested</h3>
                <p>Click below to reset your password:</p>
                <a href="${resetUrl}" style="color:blue">${resetUrl}</a>
                <br><br>
                <b>Note:</b> Link is valid for 15 minutes.
                `
            );

            await LogService.createLog({
                user_id: user.id,
                email: user.email,
                action: "FORGOT_PASSWORD",
                description: `Password reset link sent to ${email}`,
                req
            });


            return ResponseService.success(res, "Reset link sent to email", {});
        } catch (error) {
            return ResponseService.error(res, "Failed to generate reset link", 500, error);
        }
    }


    //resetPassword method
    async resetPassword(req, res) {
        try {
            const { token } = req.params;
            const { newPassword } = req.body;

            const hashedToken = crypto.createHash("sha256").update(token).digest("hex");

            const user = await User.findOne({
                resetToken: hashedToken,
                resetTokenExpire: { $gt: Date.now() }
            });

            if (!user) return ResponseService.error(res, "Token invalid or expired", 400);

            const hashedPassword = await bcrypt.hash(newPassword, 10);

            user.password = hashedPassword;
            user.resetToken = undefined;
            user.resetTokenExpire = undefined;

            await user.save();

            await LogService.createLog({
                user_id: user.id,
                email: user.email,
                action: "RESET_PASSWORD",
                description: `${user.email} reset password successfully`,
                req
            });


            return ResponseService.success(res, "Password updated successfully");
        } catch (error) {
            return ResponseService.error(res, "Password reset failed", 500, error);
        }
    }


    //changePassword method
    async changePassword(req, res) {
        try {
            const { _id } = req.user;
            const { oldPassword, newPassword } = req.body;

            const user = await User.findById(_id);
            if (!user) return ResponseService.error(res, "User not found", 404);

            const isMatch = await bcrypt.compare(oldPassword, user.password);
            if (!isMatch) {
                return ResponseService.error(res, "Old password is incorrect", 400);
            }

            const hashedPassword = await bcrypt.hash(newPassword, 10);

            user.password = hashedPassword;
            await user.save();

            await LogService.createLog({
                user_id: user.id,
                email: user.email,
                action: "CHANGE_PASSWORD",
                description: `${user.email} changed password`,
                req
            });


            return ResponseService.success(res, "Password changed successfully");
        } catch (error) {
            return ResponseService.error(res, "Something went wrong", 500, error);
        }
    }


    //insertUsersFromAPI
    async insertUsersFromAPI(req, res) {
        try {
            res.status(200).json({
                success: true,
                message: "API call received. Data storing in background...",
            });

            setImmediate(async () => {
                try {
                    const apiResponse = await axios.get("https://beta.content.rdcmedia.in/api/users");

                    console.log("✅ API Response received");

                    let users = [];

                    if (Array.isArray(apiResponse.data)) {
                        users = apiResponse.data;
                    } else if (apiResponse.data?.data && Array.isArray(apiResponse.data.data)) {
                        users = apiResponse.data.data;
                    } else if (apiResponse.data?.users && Array.isArray(apiResponse.data.users)) {
                        users = apiResponse.data.users;
                    } else {
                        console.log("❌ No valid user array found");
                        return;
                    }

                    console.log(`✅ Total users fetched: ${users.length}`);

                    // ✅ Format users including "id"
                    const formattedUsers = users.map(u => ({
                        id: u.id || null,
                        third_party_id: u.third_party_id || null,
                        third_party_sub_id: u.third_party_sub_id || null,
                        third_party_username: u.third_party_username || null,
                        access_token: u.access_token || null,
                        parent_id: u.parent_id || null,
                        name: u.name || null,
                        email: u.email || null,
                        phone: u.phone || null,
                        country_id: u.country_id || null,
                        email_verified_at: u.email_verified_at ? new Date(u.email_verified_at) : null,
                        password: u.password || null,
                        remember_token: u.remember_token || null,
                        role: u.role || null,
                    }));

                    // ✅ Insert (no upsert, duplicates allowed)
                    const chunkSize = 1000;
                    for (let i = 0; i < formattedUsers.length; i += chunkSize) {
                        const chunk = formattedUsers.slice(i, i + chunkSize);
                        try {
                            await User.insertMany(chunk, { ordered: false });
                            console.log(`✅ Inserted ${chunk.length} users`);
                        } catch (err) {
                            console.log("⚠️ InsertMany error (duplicates ignored):", err.message);
                        }
                    }

                    console.log(`✅ All users inserted successfully: ${formattedUsers.length}`);

                } catch (err) {
                    console.log("❌ Background insert error:", err.message);
                }
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                message: "Something went wrong",
                error
            });
        }
    }

    //insertArtistsFromAPI
    async insertArtistsFromAPI(req, res) {
        try {
            res.status(200).json({
                success: true,
                message: "Artist import received. Inserting in background..."
            });

            setImmediate(async () => {
                try {
                    const apiResponse = await axios.get("https://beta.content.rdcmedia.in/api/artists");

                    console.log("✅ Artist API Response received");

                    let artists = [];

                    if (Array.isArray(apiResponse.data)) {
                        artists = apiResponse.data;
                    } else if (apiResponse.data?.data && Array.isArray(apiResponse.data.data)) {
                        artists = apiResponse.data.data;
                    } else {
                        console.log("❌ No valid artist array found");
                        return;
                    }

                    console.log(`✅ Total artists fetched: ${artists.length}`);

                    // ✅ Format Data
                    const formattedArtists = artists.map(a => ({
                        id: a.id,
                        created_by: a.created_by || null, // user id
                        name: a.name || null,
                        artist_image: a.artist_image || null,
                        artist_image_url: a.artist_image_url || null,
                        apple_image: a.apple_image || null,
                        youtube_image_url: a.youtube_image_url || null,
                        youtube_link: a.youtube_link || null,
                        email: a.email || null,
                        sound_cloud: a.sound_cloud || null,
                        twitter: a.twitter || null,
                        facebook: a.facebook || null,
                        facebook_profile_id: a.facebook_profile_id || null,
                        instagram: a.instagram || null,
                        instagram_profile_id: a.instagram_profile_id || null,
                        youtube: a.youtube || null,
                        brandcamp: a.brandcamp || null,
                        website: a.website || null,
                        isrc: a.isrc || null,
                        is_on_spotfy: a.is_on_spotfy || 0,
                        is_on_apple: a.is_on_apple || 0,
                        spotfy_link: a.spotfy_link || null,
                        apple_link: a.apple_link || null,
                    }));

                    // ✅ Insert in chunks
                    const chunkSize = 1000;
                    for (let i = 0; i < formattedArtists.length; i += chunkSize) {
                        const chunk = formattedArtists.slice(i, i + chunkSize);

                        try {
                            await Artist.insertMany(chunk, { ordered: false });
                            console.log(`✅ Inser­ted ${chunk.length} artists`);
                        } catch (err) {
                            console.log("⚠️ InsertMany error:", err.message);
                        }
                    }

                    console.log(`✅ ✅ All artists inserted successfully: ${formattedArtists.length}`);

                } catch (err) {
                    console.log("❌ Artist background insert error:", err.message);
                }
            });

        } catch (err) {
            res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: err.message,
            });
        }
    }

    async insertReleasesFromAPI(req, res) {
        try {
            res.status(200).json({
                success: true,
                message: "Release import started in background"
            });

            setImmediate(async () => {
                try {
                    const apiResponse = await axios.get(
                        "https://beta.content.rdcmedia.in/api/releases",
                        {
                            headers: {
                                Accept: "application/json",
                                "X-API-TOKEN": process.env.RDC_API_TOKEN
                            }
                        }
                    );

                    let releases = [];

                    if (Array.isArray(apiResponse.data)) {
                        releases = apiResponse.data;
                    } else if (apiResponse.data?.data) {
                        releases = apiResponse.data.data;
                    }

                    console.log(`Total releases fetched: ${releases.length}`);

                    const formattedReleases = releases.map(r => ({
                        id: r.id,
                        sublabel_id: r.sublabel_id,
                        label_id: Number(r.label_id) || null,

                        lang: r.lang,
                        content_lang: r.content_lang,

                        title: r.title,

                        display_artist: (() => {
                            try {
                                return typeof r.display_artist === "string"
                                    ? JSON.parse(r.display_artist)
                                    : Array.isArray(r.display_artist)
                                        ? r.display_artist
                                        : r.display_artist
                                            ? [r.display_artist]
                                            : [];
                            } catch (e) {
                                return r.display_artist ? [r.display_artist] : [];
                            }
                        })(),


                        artwork: r.artwork,
                        rename_artwork: r.rename_artwork,

                        release_type: r.release_type,
                        create_type: r.create_type,

                        cat_number: r.cat_number,
                        moods: r.moods,

                        genre_id: r.genre_id,
                        subgenre_id: r.subgenre_id,

                        is_upc: r.is_upc,
                        upc_number: r.upc_number,

                        release_date: r.release_date
                            ? new Date(r.release_date)
                            : null,

                        p_line: r.p_line,
                        p_line_year: r.p_line_year,

                        isrc: r.isrc,

                        c_line: r.c_line,
                        c_line_year: r.c_line_year,

                        description: r.description,

                        on_itunes: r.on_itunes,

                        created_by: r.created_by,
                        status: r.status,
                        deleted: r.deleted,

                        published_date: r.published_date
                            ? new Date(r.published_date)
                            : null
                    }));

                    // Insert in chunks
                    const chunkSize = 1000;
                    for (let i = 0; i < formattedReleases.length; i += chunkSize) {
                        const chunk = formattedReleases.slice(i, i + chunkSize);
                        try {
                            await Release.insertMany(chunk, { ordered: false });
                            console.log(`Inserted ${chunk.length} releases`);
                        } catch (err) {
                            console.log(" InsertMany error:", err.message);
                        }
                    }

                    console.log(" All releases inserted successfully");

                } catch (err) {
                    console.log(" Release background insert error:", err.message);
                }
            });

        } catch (err) {
            res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: err.message
            });
        }
    }


    async insertTracksFromAPI(req, res) {
        try {
            res.status(200).json({
                success: true,
                message: "Track import received. Inserting in background...",
            });

            setImmediate(async () => {
                try {
                    const apiResponse = await axios.get(
                        "https://beta.content.rdcmedia.in/api/tracks?per_page=all",
                        {
                            headers: {
                                Accept: "application/json",
                                "X-API-TOKEN": "WXdaOTRyZk9PbVF0MkdxYWVEMVNBVk03WDVYZmdRWTQxVzRvdnlrZw==",
                            },
                        }
                    );

                    let tracks = [];

                    if (Array.isArray(apiResponse.data)) {
                        tracks = apiResponse.data;
                    } else if (apiResponse.data?.data) {
                        tracks = apiResponse.data.data;
                    }

                    console.log(` Total tracks fetched: ${tracks.length}`);

                    const formattedTracks = tracks.map(t => ({
                        id: t.id,
                        release_id: t.release_id || null,
                        serial_number: t.serial_number || null,
                        position: t.position || null,
                        disc: t.disc || null,

                        crbt_time: t.crbt_time || null,
                        crbt_seconds_total: Number(t.crbt_seconds_total || 0),

                        artists: t.artists
                            ? t.artists.toString().split(",").map(Number)
                            : [],

                        display_artist: t.display_artist
                            ? t.display_artist.split(",").map(a => a.trim())
                            : [],

                        feature_artist: t.feature_artist
                            ? t.feature_artist.split(",").map(a => a.trim())
                            : [],

                        title: t.title || null,

                        mix_version: t.mix_version || null,
                        remixer: t.remixer || null,
                        is_remix: t.is_remix || 0,

                        orchestra: t.orchestra || null,
                        arranger: t.arranger || null,
                        actor: t.actor || null,
                        conductor: t.conductor || null,
                        composer: t.composer || null,
                        producer: t.producer || null,
                        lyricist: t.lyricist || null,

                        genre_id: t.genre_id || null,
                        subgenre_id: t.subgenre_id || null,

                        publisher: t.publisher || null,
                        contributors: t.contributors || null,

                        have_isrc: t.have_isrc || 0,
                        isrc_number: t.isrc_number || null,

                        is_dolby: t.is_dolby || 0,
                        dolby_isrc: t.dolby_isrc || null,
                        dolby_audio: t.dolby_audio || null,

                        track_lyrics: t.track_lyrics || null,
                        lyrics_text: t.lyrics_text || null,

                        sold_with_album: t.sold_with_album || 0,
                        explicit: t.explicit || 0,

                        start_time: t.start_time || null,
                        end_time: t.end_time || null,

                        price: t.price || null,

                        audio_files: (() => {
                            try {
                                return typeof t.audio_files === "string"
                                    ? JSON.parse(t.audio_files)
                                    : [];
                            } catch {
                                return [];
                            }
                        })(),

                        crbt_clip: t.crbt_clip || null,
                        original_audio_name: t.original_audio_name || null,

                        duration: t.duration || null,
                    }));

                    const chunkSize = 1000;

                    for (let i = 0; i < formattedTracks.length; i += chunkSize) {
                        const chunk = formattedTracks.slice(i, i + chunkSize);
                        try {
                            await Track.insertMany(chunk, { ordered: false });
                            console.log(`Inserted ${chunk.length} tracks`);
                        } catch (err) {
                            console.log(" Track insert error:", err.message);
                        }
                    }

                    console.log(" All tracks inserted successfully");
                } catch (err) {
                    console.log("Track background insert error:", err.message);
                }
            });
        } catch (err) {
            res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: err.message,
            });
        }
    }


}

module.exports = new AuthController();
