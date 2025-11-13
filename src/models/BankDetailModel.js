const mongoose = require("mongoose");

const BankDetailSchema = new mongoose.Schema({
    user_id: {
        type: Number,
        ref: "User",
        default: null
    },

    paymentMethod: {
        type: String,
        enum: ["bank", "paypal", "upi"],
        required: true
    },

    bankName: {
        type: String
    },
    accountHolderName: {
        type: String
    },
    accountNumber: {
        type: String
    },
    ifscRouting: {
        type: String

    },
    swiftCode: {
        type: String
    },
    branch: {
        type: String
    },

    paypalEmail: {
        type: String
    },

    upiId: {
        type: String
    }
}, {
    timestamps: true,
    versionKey: false
});

module.exports = mongoose.model("BankDetail", BankDetailSchema);
