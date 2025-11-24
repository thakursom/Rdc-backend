const path = require("path");
const fs = require("fs");

const XLSX = require("xlsx");
const RevenueUpload = require("../models/RevenueUploadModel");
const AppleRevenue = require("../models/AppleRevenueModel");
const SpotifyRevenue = require("../models/SpotifyRevenueModel");
const GaanaRevenue = require("../models/GaanaRevenueModel");
const JioSaavanRevenue = require("../models/JioSaavanRevenueModel");
const FacebookRevenue = require("../models/FacebookRevenueModel");
const AmazonRevenue = require("../models/AmazonRevenueModel");
const TikTokRevenue = require("../models/TikTokRevenueModel");



class revenueUploadController {

    constructor() { }

    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {

            const { userId } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            // Save file metadata
            const RevenueUploads = await RevenueUpload.create({
                user_id: userId,
                platform,
                periodFrom,
                periodTo,
                fileName: req.file.filename,
                filePath: req.file.path,
                fileExt: req.file.mimetype,
            });

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // Save into platform collection
            if (platform === "Spotify") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await SpotifyRevenue.insertMany(rows);

            } else if (platform === "AppleItunes") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await AppleRevenue.insertMany(rows);

            } else if (platform === "Gaana") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await GaanaRevenue.insertMany(rows);

            } else if (platform === "JioSaavan") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await JioSaavanRevenue.insertMany(rows);

            } else if (platform === "Facebook") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await FacebookRevenue.insertMany(rows);

            } else if (platform === "Amazon") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));
                console.log("rows", rows);
                await AmazonRevenue.insertMany(rows);

            } else if (platform === "TikTok") {
                const rows = jsonData.map(row => ({
                    uploadId: RevenueUploads._id,
                    ...row
                }));

                await TikTokRevenue.insertMany(rows);

            }

            res.json({
                success: true,
                message: "File uploaded and processed successfully",
                uploadId: RevenueUploads._id
            });

        } catch (error) {
            console.log(error);
            res.status(500).json({ error: error.message });
        }
    }


    //getAllRevenueUploads method
    async getAllRevenueUploads(req, res, next) {
        try {
            let { page = 1, limit = 20, platform } = req.query;

            page = parseInt(page);
            limit = parseInt(limit);

            const query = {};

            // Optional filter by platform
            if (platform) {
                query.platform = platform;
            }

            const skip = (page - 1) * limit;

            const [data, total] = await Promise.all([
                RevenueUpload.find(query)
                    .sort({ createdAt: -1 })
                    .skip(skip)
                    .limit(limit),

                RevenueUpload.countDocuments(query)
            ]);

            return res.json({
                success: true,
                message: "Revenue uploads fetched successfully",
                currentPage: page,
                totalPages: Math.ceil(total / limit),
                totalRecords: total,
                data
            });

        } catch (error) {
            console.log(error);
            return res.status(500).json({ success: false, error: error.message });
        }
    }


    async downloadRevenueFile(req, res, next) {
        try {
            const { filePath } = req.query;

            if (!filePath) {
                return res.status(400).json({ success: false, message: "File path required" });
            }

            // Convert to absolute path
            const absolutePath = path.resolve(filePath);

            // Check if file exists
            if (!fs.existsSync(absolutePath)) {
                return res.status(404).json({ success: false, message: "File not found" });
            }

            return res.download(absolutePath);
        } catch (error) {
            console.error("Download error:", error);
            return res.status(500).json({ success: false, message: "Error downloading file" });
        }

    }
}

module.exports = new revenueUploadController();