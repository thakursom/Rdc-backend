const mongoose = require("mongoose");

const UserSchema = new mongoose.Schema({
    id: {
        type: Number,
        default: null,
    },
    third_party_id: {
        type: Number,
        default: null,
    },
    third_party_sub_id: {
        type: Number,
        default: null,
    },
    third_party_username: {
        type: String,
        default: null
    },
    access_token: {
        type: String,
        default: null
    },
    parent_id: {
        type: Number,
        default: null
    },
    name: {
        type: String,
    },
    email: {
        type: String,
        unique: true
    },
    phone: {
        type: String,
        default: null
    },
    country_id: {
        type: Number,
        default: null
    },
    email_verified_at: {
        type: Date,
        default: null
    },
    password: {
        type: String,
    },
    remember_token: {
        type: String,
        default: null
    },
    role: {
        type: String,
        required: true,
    },
    amount: {
        type: Number,
        default: 1010
    },
    resetToken: {
        type: String,
        default: null
    },
    resetTokenExpire: {
        type: Date,
        default: null
    }

}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model("User", UserSchema);
