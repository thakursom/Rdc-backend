const XLSX = require("xlsx");

const RevenueUpload = require("../models/RevenueUploadModel");
const AppleRevenue = require("../models/appleRevenueModel");
const SpotifyRevenue = require("../models/spotifyRevenueModel");
const GaanaRevenue = require("../models/gaanaRevenueModel");
const JioSaavanRevenue = require("../models/jioSaavanRevenueModel");
const FacebookRevenue = require("../models/facebookRevenueModel");
const AmazonRevenue = require("../models/amazonRevenueModel");
const TikTokRevenue = require("../models/tikTokRevenueModel");
const TempReport = require("../models/tempReportModel");
const TblReport2025 = require("../models/tblReport2025Model");




class revenueUploadController {

    constructor() { }

    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {
            // const { userId } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            const relativePath = `uploads/revenues/${req.file.filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            const RevenueUploads = await RevenueUpload.create({
                user_id: 0,
                platform,
                periodFrom,
                periodTo,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // const rows = jsonData.map(row => ({
            //     uploadId: RevenueUploads._id,
            //     ...row
            // }));

            const mappedRows = jsonData.map(r => {
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.elected_isrc || null,
                        release: r.track_title || null,
                        track_title: r.track_title || null,
                        track_artist: r.track_artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.event_count_1 || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album name"] || null,
                        track_title: r["Track name"] || null,
                        track_artist: r["Artist name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r.Total || null,
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r["territory code"] || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Plays"] || null,
                        sale_type: null,
                        net_total: r[" Total Revenue"] || null,
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    obj = {
                        retailer: "jio_savan",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Streams"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Item Title"] || null,
                        track_title: r["Item Title"] || null,
                        track_artist: r["Item Artist"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    obj = {
                        retailer: r.platforn_name,
                        label: r["Label Name"] || null,
                        upc_code: r.product_code,
                        catalogue_number: null,
                        isrc_code: r.isrc || null,
                        release: r.album || null,
                        track_title: r.song_title || null,
                        track_artist: r.artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r.content_type || null,
                        track_count: r.video_views || null,
                        sale_type: null,
                        net_total: r["INR Revenue"] || null,
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r.Album || null,
                        track_title: r["Track Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r.content_type || null,
                        track_count: r["Total Playouts"] || null,
                        sale_type: null,
                        net_total: r["Total"] || null,
                    };
                }

                const today = new Date().toISOString().split("T")[0];
                obj.date = today;
                obj.user_id = 0;
                obj.uploading_date = today;
                obj.uploadId = RevenueUploads._id;

                return obj;
            });


            // Insert platform-specific revenue table
            const modelMap = {
                Spotify: SpotifyRevenue,
                AppleItunes: AppleRevenue,
                Gaana: GaanaRevenue,
                JioSaavan: JioSaavanRevenue,
                Facebook: FacebookRevenue,
                Amazon: AmazonRevenue,
                TikTok: TikTokRevenue
            };

            if (modelMap[platform]) {
                await modelMap[platform].insertMany(mappedRows);
            }

            // Now insert the new mapped rows
            await TempReport.insertMany(mappedRows);

            return res.json({
                success: true,
                message: "File uploaded and processed successfully",
                uploadId: RevenueUploads._id,
                fileURL: fileURL
            });

        } catch (error) {
            next(error);
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
            next(error)
        }
    }

    //getRevenueById method
    async getRevenueById(req, res, next) {
        try {
            const { userId, page = 1, limit = 20 } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // const numericUserId = parseInt(userId);
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            const revenues = await TempReport.find({
                uploadId: userId
            })
                .sort({ uploading_date: -1 })
                .skip(skip)
                .limit(limitNum);

            const totalCount = await TempReport.countDocuments({
                uploadId: userId
            });

            const totalPages = Math.ceil(totalCount / limitNum);

            return res.status(200).json({
                success: true,
                message: "Revenue data retrieved successfully",
                data: revenues,
                pagination: {
                    currentPage: pageNum,
                    totalPages: totalPages,
                    totalCount: totalCount,
                    hasNext: pageNum < totalPages,
                    hasPrev: pageNum > 1
                }
            });

        } catch (error) {
            console.error("Error fetching revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //uploadTblRevenue method
    async uploadTblRevenue(req, res, next) {
        try {
            const { uploadId } = req.query;

            if (!uploadId) {
                return res.status(400).json({ success: false, message: "uploadId required" });
            }

            // Find and update the revenue upload
            const revenueUpload = await RevenueUpload.findByIdAndUpdate(
                uploadId,
                { isAccepted: true },
                { new: true }
            );

            if (!revenueUpload) {
                return res.status(404).json({
                    success: false,
                    message: "Revenue upload not found"
                });
            }

            //Get all TempReport rows for this uploadId
            const tempData = await TempReport.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({ success: false, message: "No data found for this uploadId" });
            }

            // Remove MongoDB _id field so it can be inserted fresh
            const cleanedData = tempData.map(row => {
                const { _id, ...rest } = row;
                return rest;
            });

            //Insert into TblReport2025 in bulk
            await TblReport2025.insertMany(cleanedData);

            // Delete all existing data from TempReport
            await TempReport.deleteMany({ uploadId });

            return res.status(200).json({
                success: true,
                message: "Data moved from TempReport to TblReport_2025 successfully",
                insertedCount: cleanedData.length
            });

        } catch (error) {
            console.error("uploadData error:", error);
            return res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: error.message
            });
        }
    }

}

module.exports = new revenueUploadController();