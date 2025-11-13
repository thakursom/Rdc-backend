const axios = require("axios");
const crypto = require("crypto");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");

const ResponseService = require("../services/responseService");
const sendEmail = require("../utils/sendEmail");
const User = require("../models/userModel");
const Artist = require("../models/artistModel");

class AuthController {

    constructor() { }

    //login method
    async login(req, res) {
        try {
            const { email, password } = req.body;

            const user = await User.findOne({ email });
            if (!user) return ResponseService.error(res, "User not found", 404);

            const match = await bcrypt.compare(password, user.password);
            if (!match) return ResponseService.error(res, "Invalid password", 400);

            const token = jwt.sign(
                {
                    _id: user._id,      // Mongo ID
                    userId: user.id,    // ✅ Auto-increment ID
                    role: user.role
                },
                process.env.JWT_SECRET,
                { expiresIn: "1d" }
            );

            return ResponseService.success(res, "Login successful", {
                token,
                user: {
                    name: user.name,
                    email: user.email,
                    role: user.role,
                    userId: user.id     // ✅ send with response also
                }
            });
        } catch (error) {
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

            return ResponseService.success(res, "Password updated successfully");
        } catch (error) {
            return ResponseService.error(res, "Password reset failed", 500, error);
        }
    }


    //changePassword method
    async changePassword(req, res) {
        try {
            const { id } = req.user;
            const { oldPassword, newPassword } = req.body;

            const user = await User.findById(id);
            if (!user) return ResponseService.error(res, "User not found", 404);

            const isMatch = await bcrypt.compare(oldPassword, user.password);
            if (!isMatch) {
                return ResponseService.error(res, "Old password is incorrect", 400);
            }

            const hashedPassword = await bcrypt.hash(newPassword, 10);

            user.password = hashedPassword;
            await user.save();

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

}

module.exports = new AuthController();
