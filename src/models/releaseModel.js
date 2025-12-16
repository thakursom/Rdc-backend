const mongoose = require("mongoose");

const ReleaseSchema = new mongoose.Schema({
    sublabel_id: {
        type: Number,
        default: null
    },
    label_id: {
        type: Number,
        default: null
    },

    lang: {
        type: String,
        default: null
    },
    content_lang: {
        type: String,
        default: null
    },

    title: {
        type: String,
        required: true
    },

    display_artist: {
        type: [String], // ["Gaman Santhal","Kajal Maheriya"]
        default: []
    },

    artists: {
        type: String,
        default: null
    },
    feature_artist: {
        type: String,
        default: null
    },
    remixer: {
        type: String,
        default: null
    },
    composer: {
        type: String,
        default: null
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
    producer: {
        type: String,
        default: null
    },
    conductor: {
        type: String,
        default: null
    },

    is_instrumental: {
        type: Number,
        default: 0
    },

    artwork: {
        type: String,
        default: null
    },
    rename_artwork: {
        type: String,
        default: null
    },
    apple_art: {
        type: String,
        default: null
    },
    rename_apple_art: {
        type: String,
        default: null
    },

    single_audio_file: {
        type: String,
        default: null
    },
    rename_single_audio_file: {
        type: String,
        default: null
    },

    release_type: {
        type: String,
        default: null
    },
    provided_by: {
        type: String,
        default: null
    },
    create_type: {
        type: String,
        default: null
    },

    cat_number: {
        type: String,
        default: null
    },
    moods: {
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

    is_upc: {
        type: Number,
        default: 0
    },
    upc_number: {
        type: String,
        default: null
    },

    youtube_package_id: {
        type: Number,
        default: null
    },

    release_date: {
        type: Date,
        default: null
    },
    original_rel_date: {
        type: Date,
        default: null
    },

    p_line: {
        type: String,
        default: null
    },
    p_line_year: {
        type: String,
        default: null
    },

    isrc: {
        type: String,
        default: null
    },

    c_line: {
        type: String,
        default: null
    },
    c_line_year: {
        type: String,
        default: null
    },

    parental_warning_type: {
        type: String,
        default: "0"
    },

    description: {
        type: String,
        default: null
    },

    on_itunes: {
        type: Number,
        default: 0
    },

    created_by: {
        type: Number,
        default: null
    },
    status:
    {
        type: String,
        default: "0"
    },
    deleted: {
        type: Number,
        default: 0
    },

    published_date: {
        type: Date,
        default: null
    }
},
    {
        timestamps: true,
        versionKey: false
    }
);

module.exports = mongoose.model("Release", ReleaseSchema);
