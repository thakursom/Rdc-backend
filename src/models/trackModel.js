const mongoose = require("mongoose");

const TrackSchema = new mongoose.Schema({
    id: {
        type: Number,
        required: true,
        unique: true,
        index: true
    },
    release_id: {
        type: Number,
        default: null
    },
    serial_number: {
        type: String,
        default: null
    },
    position: {
        type: Number,
        default: null
    },
    disc: {
        type: Number,
        default: null
    },

    crbt_time: {
        type: String,
        default: null
    },
    crbt_seconds_total: {
        type: Number,
        default: 0
    },

    artists: {
        type: [Number],
        default: []
    }, // "3306" → [3306]
    display_artist: {
        type: [String],
        default: []
    },
    feature_artist: {
        type: [String],
        default: []
    },

    title: {
        type: String,
        required: true
    },

    mix_version: {
        type: String,
        default: null
    },
    remixer: {
        type: String,
        default: null
    },
    is_remix: {
        type: Number,
        default: 0
    },

    orchestra: {
        type: String,
        default: null
    },
    arranger: {
        type: String,
        default: null
    },
    actor: {
        type: String,
        default: null
    },
    conductor: {
        type: String,
        default: null
    },
    composer: {
        type: String,
        default: null
    },
    producer: {
        type: String,
        default: null
    },
    lyricist: {
        type: String,
        default: null
    },

    genre_id: {
        type: Number,
        default: null
    },
    subgenre_id: {
        type: Number,
        default: null
    },

    publisher: {
        type: String,
        default: null
    },
    contributors: {
        type: String,
        default: null
    },

    have_isrc: {
        type: Number,
        default: 0
    },
    isrc_number: {
        type: String,
        default: null
    },

    is_dolby: {
        type: Number,
        default: 0
    },
    dolby_isrc: {
        type: String,
        default: null
    },
    dolby_audio: {
        type: String,
        default: null
    },

    track_lyrics: {
        type: String,
        default: null
    },
    lyrics_text: {
        type: String,
        default: null
    },

    sold_with_album: {
        type: Number,
        default: 0
    },
    explicit: {
        type: Number,
        default: 0
    },

    start_time: {
        type: String,
        default: null
    },
    end_time: {
        type: String,
        default: null
    },

    price: {
        type: Number,
        default: null
    },

    audio_files: {
        type: [String],
        default: []
    },
    crbt_clip: {
        type: String,
        default: null
    },
    original_audio_name: {
        type: String,
        default: null
    },

    duration: {
        type: String,
        default: null
    },
},
    {
        timestamps: true,
        versionKey: false,
    }
);

module.exports = mongoose.model("Track", TrackSchema);
