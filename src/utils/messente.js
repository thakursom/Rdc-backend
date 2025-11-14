const axios = require("axios");

const sendWhatsappMessage = async (to, message) => {
    try {
        const INSTANCE_ID = process.env.WAPULSE_INSTANCE_ID;
        const API_TOKEN = process.env.WAPULSE_API_TOKEN;

        const url = `https://api.wapulse.com/v1/messages/send-text`;

        const payload = {
            instance_id: INSTANCE_ID,
            token: API_TOKEN,
            to,                 // WhatsApp number: 91XXXXXXXX
            message
        };

        const res = await axios.post(url, payload);
        console.log("WhatsApp Sent:", res.data);
        return res.data;

    } catch (error) {
        console.error("WhatsApp Error:", error?.response?.data || error.message);
        throw new Error("Failed to send WhatsApp message");
    }
};

module.exports = sendWhatsappMessage;
