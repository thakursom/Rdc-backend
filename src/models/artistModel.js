const mongoose = require("mongoose");

const ArtistSchema = new mongoose.Schema({
    id: {
        type: Number,
        default: null,
    },
    created_by: {
        type: Number,
        ref: "User",
        default: null
    },
    name: {
        type: String,
        required: true
    },
    artist_image: {
        type: String,
        default: null
    },
    artist_image_url: {
        type: String,
        default: null
    },
    apple_image: {
        type: String,
        default: null
    },
    youtube_image_url: {
        type: String,
        default: null
    },
    youtube_link: {
        type: String,
        default: null
    },
    email: {
        type: String,
        default: null
    },
    sound_cloud: {
        type: String,
        default: null
    },
    twitter: {
        type: String,
        default: null
    },
    facebook: {
        type: String,
        default: null
    },
    facebook_profile_id: {
        type: String,
        default: null
    },
    instagram: {
        type: String,
        default: null
    },
    instagram_profile_id: {
        type: String,
        default: null
    },
    youtube: {
        type: String,
        default: null
    },
    brandcamp: {
        type: String,
        default: null
    },
    website: {
        type: String,
        default: null
    },
    isrc: {
        type: String,
        default: null
    },
    is_on_spotify: {
        type: Number,
        default: 0
    },
    is_on_apple: {
        type: Number,
        default: 0
    },
    spotify_link: {
        type: String,
        default: null
    },
    apple_link: {
        type: String,
        default: null
    }
},
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("Artist", ArtistSchema);
