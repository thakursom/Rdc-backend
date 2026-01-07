const mongoose = require("mongoose");
const fs = require('fs');
const path = require('path');
const XLSX = require("xlsx");
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const ExcelJS = require('exceljs');

const { excelSerialToISODate } = require("../utils/dateUtils");
const LogService = require("../services/logService");
const User = require("../models/userModel");
const Release = require("../models/releaseModel");
const Contract = require("../models/contractModel");
const RevenueUpload = require("../models/RevenueUploadModel");
const TempReport = require("../models/tempReportModel");
const TblReport2025 = require("../models/tblReport2025Model");
const AudioStreamingReportHistory = require("../models/audioStreamingReportHistoryModel");
const YoutubeReportHistory = require("../models/youtubeReportHistoryModel");
const YouTube = require("../models/youtubeModel");
const TempYoutube = require("../models/tempYoutubeModel");
const RevenueSummary = require("../models/revenueSummaryModel");

const monthMap = {
    Jan: '01', Feb: '02', Mar: '03', Apr: '04',
    May: '05', Jun: '06', Jul: '07', Aug: '08',
    Sep: '09', Oct: '10', Nov: '11', Dec: '12'
};

const getDateFromMonthYear = (month, year) => {
    if (!month || !year) return null;
    const m = monthMap[month];
    return m ? `${year}-${m}-01` : null;
};

const getLast12Months = () => {
    const months = [];
    const now = new Date();

    for (let i = 11; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
        months.push(key);
    }
    return months;
};

const BATCH_SIZE = 1000;


class revenueUploadController {

    constructor() {

        this.processPendingReports = this.processPendingReports.bind(this);
        this.processAudioStreamingReport = this.processAudioStreamingReport.bind(this);
        this.processPendingYoutubeReports = this.processPendingYoutubeReports.bind(this);
        this.processYoutubeReport = this.processYoutubeReport.bind(this);
        this.importRevenueFromJson = this.importRevenueFromJson.bind(this);
        this.insertBatch = this.insertBatch.bind(this);
        this.importYoutubeRevenueFromJson = this.importYoutubeRevenueFromJson.bind(this);
        this.insertYoutubeBatch = this.insertYoutubeBatch.bind(this);
        this.uploadTblRevenue = this.uploadTblRevenue.bind(this);
        this.calculateRevenueSummary = this.calculateRevenueSummary.bind(this);
        this.calculateRevenueForSuperAdminandManager = this.calculateRevenueForSuperAdminandManager.bind(this);
    }


    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            const relativePath = `uploads/revenues/${req.file.filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            const labelIdentifiers = new Set();
            const rowsWithData = [];

            jsonData.forEach(r => {
                let isrcCode = null;
                let labelCodeFromFile = null;
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    isrcCode = r.elected_isrc;
                    labelCodeFromFile = r["Label ID"];
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label ID"];
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label ID"];
                    obj = {
                        retailer: "Jio Saavn",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    isrcCode = r.Isrc;
                    labelCodeFromFile = r["Label ID"];
                    obj = {
                        retailer: r.Platforn_Name,
                        label: r["Label Name"] || null,
                        upc_code: r.Product_Code,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r.Song_Title || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Views || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "SoundRecording") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: platform || null,
                        track_artist: r.Artist || null,
                        type: r["Type"] || null,
                        asset_id: r["Asset ID"] || null,
                        country: r["Country"] || null,
                        asset_title: r["Asset Title"] || null,
                        isrc_code: isrcCode || null,
                        upc_code: r["UPC"] || null,
                        custom_id: r["Custom ID"] || null,
                        asset_channel_id: r["Asset Channel ID"] || null,
                        channel_name: r["Channel Name"] || null,
                        label_name: r["Label Name"] || null,
                        label_code: r["Label Code"] || null,
                        total_play: r["Owned Views"] ? Number(r["Owned Views"]) : null,
                        partner_revenue: r["Partner Revenue"] || null,
                        inr_rate: r["INR Rate"] ? Number(r["INR Rate"]) : null,
                        total_revenue: r["Total Revenue"] ? Number(r["Total Revenue"]) : null,
                        label_shared: r["Label Shared"] ? Number(r["Label Shared"]) : null,
                        sub_label: r["Label Name"] || null,
                        sub_label_share: r["Sub Label Code"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                        content_type: null,
                        claim_type: null
                    };
                } else if (platform === "YouTubeArtTrack") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: platform || null,
                        track_artist: r.Artist || null,
                        type: r["Type"] || null,
                        asset_id: r["Asset ID"] || null,
                        country: r["Country"] || null,
                        content_type: r["Content Type"] || null,
                        claim_type: r["Claim Type"] || null,
                        asset_title: r["Asset Title"] || null,
                        video_duration_sec: r["Video Duration (sec)"] || null,
                        category: r["Category"] || null,
                        isrc_code: isrcCode || null,
                        upc_code: r["UPC"] || null,
                        custom_id: r["Custom ID"] || null,
                        video_link: r["Video Link"] || null,
                        channel_link: r["Channel Link"] || null,
                        asset_channel_id: r["Asset Channel ID"] || null,
                        channel_name: r["Channel Name"] || null,
                        label_name: r["Label Name"] || null,
                        label_code: r["Label Code"] || null,
                        total_play: r["Owned Views"] ? Number(r["Owned Views"]) : null,
                        partner_revenue: r["Partner Revenue"] || null,
                        inr_rate: r["INR Rate"] ? Number(r["INR Rate"]) : null,
                        total_revenue: r["Total Revenue"] ? Number(r["Total Revenue"]) : null,
                        label_shared: r["Label Shared"] ? Number(r["Label Shared"]) : null,
                        sub_label: r["Sub Label"] || null,
                        sub_label_share: null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubePartnerChannel" || platform === "YouTubeRDCChannel") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: platform || null,
                        track_artist: r.Artist || null,
                        type: r["Type"] || null,
                        asset_id: r["Asset Id"] || null,
                        country: r["Country"] || null,
                        content_type: r["Content Type"] || null,
                        claim_type: r["Claim Type"] || null,
                        asset_title: r["Asset Title"] || null,
                        video_duration_sec: r["Video Duration (sec)"] || null,
                        custom_id: r["Custom ID"] || null,
                        video_link: r["Video Link"] || null,
                        channel_link: r["Channel Link"] || null,
                        asset_channel_id: r["Asset Channel ID"] || null,
                        channel_name: r["Channel Display Name"] || null,
                        label_name: r["Label Name"] || null,
                        label_code: r["Label Code"] || null,
                        total_play: r["Owned Views"] ? Number(r["Owned Views"]) : null,
                        partner_revenue: r["Partner Revenue"] || null,
                        usd: r["USD"] ? Number(r["USD"]) : null,
                        total_revenue: r["Total INR"] ? Number(r["Total INR"]) : null,
                        label_share: r["Label Share"] ? Number(r["Label Share"]) : null,
                        sub_label: r["Sub Label"] || null,
                        sub_label_share: r["Sub Label Code"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeVideoClaim") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: platform || null,
                        track_artist: r.Artist || null,
                        type: r["Type"] || null,
                        asset_id: r["Asset Id"] || null,
                        country: r["Country"] || null,
                        content_type: r["Content Type"] || null,
                        claim_type: r["Claim Type"] || null,
                        asset_title: r["Asset Title"] || null,
                        video_duration_sec: r["Video Duration (sec)"] || null,
                        custom_id: r["Custom ID"] || null,
                        video_link: r["Video Link"] || null,
                        channel_link: r["Channel LinK"] || null,
                        asset_channel_id: r["Asset Channel ID"] || null,
                        channel_name: r["Channel Display Name"] || null,
                        label_name: r["Label Name"] || null,
                        label_code: r["Label Code"] || null,
                        total_play: r["Owned Views"] ? Number(r["Owned Views"]) : null,
                        partner_revenue: r["Partner Revenue"] || null,
                        usd: r["USD"] ? Number(r["USD"]) : null,
                        total_revenue: r["Total INR"] ? Number(r["Total INR"]) : null,
                        label_share: r["Label Share"] ? Number(r["Label Share"]) : null,
                        sub_label: r["Sub Label"] || null,
                        sub_label_share: r["Sub Label Code"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                        category: r["Category"] || null,
                    };
                } else if (platform === "YTPremiumRevenue") {
                    isrcCode = r.ISRC;
                    labelCodeFromFile = r["Label Code"];
                    obj = {
                        retailer: platform || null,
                        type: r["Type"] || null,
                        asset_id: r["Asset Id"] || null,
                        country: r["Country"] || null,
                        content_type: r["Content Type"] || null,
                        claim_type: r["Claim Type"] || null,
                        asset_title: r["Asset Title"] || null,
                        video_duration_sec: r["Video Duration (sec)"] || null,
                        custom_id: r["Custom ID"] || null,
                        video_link: r["Video Link"] || null,
                        channel_link: r["Channel Link"] || null,
                        asset_channel_id: r["Asset Channel ID"] || null,
                        channel_name: r["Channel Display Name"] || null,
                        label_name: r["Label Name"] || null,
                        label_code: r["Label Code"] || null,
                        total_play: r["Owned Views"] ? Number(r["Owned Views"]) : null,
                        partner_revenue: r["Partner Revenue"] || null,
                        usd: r["USD"] ? Number(r["USD"]) : null,
                        total_revenue: r["Total INR"] ? Number(r["Total INR"]) : null,
                        label_share: r["Label Share"] ? Number(r["Label Share"]) : null,
                        sub_label: r["Sub Label Name"] || null,
                        sub_label_share: r["Sub Label Code"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                        category: r["Category"] || null,
                    };
                }

                if (labelCodeFromFile) {
                    labelIdentifiers.add(String(labelCodeFromFile).trim());
                }

                rowsWithData.push({
                    data: obj,
                    labelCodeFromFile: labelCodeFromFile || null,
                });
            });

            const labelToUserIdMap = {};
            if (labelIdentifiers.size > 0) {
                const users = await User.find({
                    third_party_username: { $in: Array.from(labelIdentifiers) }
                }).select('id third_party_username').lean();

                users.forEach(user => {
                    labelToUserIdMap[user.third_party_username] = user.id;
                });
            }

            const RevenueUploads = await RevenueUpload.create({
                user_id: userId,
                platform,
                periodFrom: periodFrom || null,
                periodTo: periodTo || null,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Prepare final mapped rows with label_id
            const mappedRows = [];
            const today = new Date().toISOString().split("T")[0];

            rowsWithData.forEach(row => {
                let assignedUserId = 0;

                if (row.labelCodeFromFile && labelToUserIdMap[String(row.labelCodeFromFile).trim()]) {
                    assignedUserId = labelToUserIdMap[String(row.labelCodeFromFile).trim()];
                }

                const finalRow = {
                    ...row.data,
                    user_id: assignedUserId,
                    uploading_date: today,
                    uploadId: RevenueUploads._id
                };

                mappedRows.push(finalRow);
            });

            const youtubePlatforms = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            if (youtubePlatforms.includes(platform)) {
                await TempYoutube.insertMany(mappedRows);
            } else {
                await TempReport.insertMany(mappedRows);
            }

            await LogService.createLog({
                user_id: userId,
                email,
                action: `ADD_REVENUE_FOR_${platform}`,
                description: `${platform} revenue uploaded successfully`,
                newData: mappedRows,
                req
            });

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

            if (platform) {
                query.platform = platform;
            }

            const skip = (page - 1) * limit;

            const aggregationPipeline = [
                { $match: query },
                {
                    $lookup: {
                        from: 'users',
                        localField: 'user_id',
                        foreignField: 'id',
                        as: 'userDetails'
                    }
                },
                {
                    $addFields: {
                        username: {
                            $arrayElemAt: ['$userDetails.name', 0]
                        }
                    }
                },
                {
                    $project: {
                        userDetails: 0
                    }
                },
                { $sort: { createdAt: -1 } },
                { $skip: skip },
                { $limit: limit }
            ];

            const [data, total] = await Promise.all([
                RevenueUpload.aggregate(aggregationPipeline),
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
                    message: "userId is required"
                });
            }

            const pageNum = parseInt(page, 10);
            const limitNum = parseInt(limit, 10);
            const skip = (pageNum - 1) * limitNum;

            // Step 1: Find the RevenueUpload to determine the platform
            const revenueUpload = await RevenueUpload.findById(userId).select('platform').lean();

            if (!revenueUpload) {
                return res.status(404).json({
                    success: false,
                    message: "Upload record not found"
                });
            }

            const { platform } = revenueUpload;

            // Step 2: Define YouTube platforms that use TempYoutube
            const youtubePlatforms = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            // Step 3: Choose the correct model
            const Model = youtubePlatforms.includes(platform) ? TempYoutube : TempReport;


            // Step 4: Fetch data with pagination
            const revenues = await Model.find({
                uploadId: userId
            })
                .sort({ uploading_date: -1 })
                .skip(skip)
                .limit(limitNum)
                .lean();

            const totalCount = await Model.countDocuments({
                uploadId: userId
            });

            const totalPages = Math.ceil(totalCount / limitNum);

            return res.status(200).json({
                success: true,
                message: "Revenue data retrieved successfully",
                data: revenues,
                pagination: {
                    currentPage: pageNum,
                    totalPages,
                    totalCount,
                    hasNext: pageNum < totalPages,
                    hasPrev: pageNum > 1,
                    limit: limitNum
                }
            });

        } catch (error) {
            console.error("Error fetching revenue by userId:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //uploadTblRevenue method
    async uploadTblRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { uploadId } = req.query;

            if (!uploadId) {
                return res.status(400).json({
                    success: false,
                    message: "uploadId required"
                });
            }

            // Accept the upload
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

            const { platform } = revenueUpload;

            // Define YouTube platforms
            const youtubePlatforms = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            // Choose the correct temp and final models
            const isYouTube = youtubePlatforms.includes(platform);
            const TempModel = isYouTube ? TempYoutube : TempReport;
            const FinalModel = isYouTube ? YouTube : TblReport2025;

            // Fetch Temp data
            const tempData = await TempModel.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({
                    success: false,
                    message: "No data found for this uploadId"
                });
            }

            const cleanedData = tempData.map(({ _id, ...rest }) => rest);

            // Collect unique user_ids
            const userIds = [...new Set(
                cleanedData
                    .map(r => r.user_id)
                    .filter(id => id !== null && id !== 0)
            )];

            // Fetch active contracts
            const contracts = await Contract.find({
                user_id: { $in: userIds },
                status: "active"
            }).lean();

            // Helper for safe date conversion
            const toDate = (d) => {
                if (!d) return null;
                const parsed = new Date(d);
                return isNaN(parsed) ? null : parsed;
            };

            // Apply percentage to each row
            const finalData = cleanedData.map(row => {
                const rowDate = toDate(row.date);
                let percentage = 0;

                if (rowDate) {
                    const matchedContract = contracts.find(contract =>
                        contract.user_id === row.user_id &&
                        rowDate >= new Date(contract.startDate) &&
                        rowDate <= new Date(contract.endDate)
                    );

                    if (matchedContract) {
                        percentage = matchedContract.labelPercentage || 0;
                    }
                }

                return {
                    ...row,
                    percentage
                };
            });

            // Insert into FinalModel
            await FinalModel.insertMany(finalData);
            // await this.calculateRevenueSummary(userId);
            const affectedUserIds = [...new Set(finalData.map(row => row.user_id).filter(id => id !== null && id !== undefined && id !== 0))];
            console.log("affectedUserIds", affectedUserIds);

            for (const id of affectedUserIds) {
                await this.calculateRevenueSummary(id);  // Pass each affected user_id
            }

            await this.calculateRevenueForSuperAdminandManager();

            // Clear TempModel
            await TempModel.deleteMany({ uploadId });

            // Logging (use platform for consistency, fallback to retailer if available)
            const logPlatform = tempData[0].retailer || platform;

            await LogService.createLog({
                user_id: userId,
                email,
                action: `REVENUE_ADDED_IN_TBLREPORT_FOR_${logPlatform}`,
                description: `${logPlatform} revenue uploaded successfully in ${isYouTube ? 'YouTube' : 'tbl_report'}`,
                newData: finalData,
                req
            });

            return res.status(200).json({
                success: true,
                message: `Data moved from ${isYouTube ? 'TempYoutube' : 'TempReport'} to ${isYouTube ? 'YouTube' : 'TblReport_2025'} successfully`,
                insertedCount: finalData.length
            });

        } catch (error) {
            console.error("uploadTblRevenue error:", error);
            return res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: error.message
            });
        }
    }

    // getAudioStreamingRevenueSummary method
    async getAudioStreamingRevenueSummary(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter (same as original) ===
            const filter = {};
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = ["Apple Music", "Spotify", "Gaana", "Jio Saavn", "Facebook", "Amazon", "TikTok"];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            // if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            // if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            // if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // === Step 1: Daily aggregation for revenue and streams ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } },
                        streamsNum: { $convert: { input: "$track_count", to: "long", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" },
                        dailyStreams: { $sum: "$streamsNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        dailyStreams: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Fetch contracts and apply deductions ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];
            const contracts = uniqueUserIds.length > 0
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            // Apply deductions
            let totalDeductedRevenue = 0;
            let totalStreams = 0;
            let entriesWithDeduction = 0;
            let sumDeductionPercent = 0;

            dailyData.forEach(item => {
                let deducted = item.dailyRevenue;
                let percentage = 0;
                let applied = false;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        applied = true;
                        break;
                    }
                }

                if (applied) {
                    entriesWithDeduction++;
                    sumDeductionPercent += percentage;
                }

                totalDeductedRevenue += deducted;
                totalStreams += item.dailyStreams;
            });

            const avgDeductionPercentage = entriesWithDeduction > 0 ? sumDeductionPercent / entriesWithDeduction : 0;

            // === Step 3: Charts data ===
            const chartPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $facet: {
                        byMonth: [
                            { $group: { _id: { $dateToString: { format: "%b %Y", date: { $dateFromString: { dateString: "$date" } } } }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { _id: 1 } },
                            { $project: { month: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byPlatform: [
                            { $group: { _id: { $ifNull: ["$retailer", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $project: { platform: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byCountry: [
                            { $group: { _id: { $ifNull: ["$territory", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 },
                            { $project: { country: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ]
                    }
                }
            ];

            const [chartResult] = await TblReport2025.aggregate(chartPipeline).allowDiskUse(true);

            // Apply global deduction ratio to charts
            const grossTotal = chartResult.byMonth.reduce((s, m) => s + m.revenue, 0);
            const deductionRatio = grossTotal > 0 ? totalDeductedRevenue / grossTotal : 1;

            const revenueByMonth = Object.fromEntries(
                chartResult.byMonth.map(m => [m.month, Number((m.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByChannel = Object.fromEntries(
                chartResult.byPlatform.map(p => [p.platform, Number((p.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByCountry = Object.fromEntries(
                chartResult.byCountry.map(c => [c.country, Number((c.revenue * deductionRatio).toFixed(2))])
            );

            // === Response ===
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: totalStreams || 0,
                        totalRevenue: Number(totalDeductedRevenue.toFixed(2)),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: Number(avgDeductionPercentage.toFixed(2)),
                        entriesWithDeduction,
                        totalEntries: dailyData.length
                    },
                    revenueByMonth,
                    revenueByChannel,
                    revenueByCountry
                }
            });

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueSummary:", error);
            next(error);
        }
    }

    // getAudioStreamingRevenueReports method
    async getAudioStreamingRevenueReports(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory,
                page = 1, limit = 10
            } = req.query;

            const { role, userId } = req.user;

            const filter = {};

            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = ["Apple Music", "Spotify", "Gaana", "Jio Saavn", "Facebook", "Amazon", "TikTok"];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skipNum = (pageNum - 1) * limitNum;

            const hasGrouping = releases === "true" || artist === "true" || track === "true" || territory === "true";

            let pipeline = [
                { $match: filter },
                {
                    $addFields: {
                        netRevenue: {
                            $multiply: [
                                { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } },
                                { $divide: [{ $subtract: [100, { $ifNull: ["$percentage", 0] }] }, 100] }
                            ]
                        }
                    }
                }
            ];

            let totalRecords = 0;

            if (hasGrouping) {
                let groupId = {};

                if (artist === "true") groupId.artist = { $ifNull: ["$track_artist", "Unknown Artist"] };
                if (releases === "true") groupId.release = { $ifNull: ["$release", "Unknown Release"] };
                if (track === "true") groupId.isrc_code = { $ifNull: ["$isrc_code", "Unknown"] };
                if (territory === "true") groupId.territory = { $ifNull: ["$territory", "Global"] };

                pipeline.push(
                    {
                        $group: {
                            _id: groupId,
                            revenue: { $sum: "$netRevenue" },
                            sampleDate: { $first: "$date" },
                            samplePlatform: { $first: "$retailer" },
                            sampleArtist: { $first: "$track_artist" },
                            sampleRelease: { $first: "$release" },
                            sampleISRC: { $first: "$isrc_code" }
                        }
                    },
                    { $sort: { revenue: -1 } }
                );

                // Get total count
                const countPipeline = [...pipeline, { $count: "total" }];
                const countResult = await TblReport2025.aggregate(countPipeline).allowDiskUse(true);
                totalRecords = countResult[0]?.total || 0;

                // Add pagination
                pipeline.push({ $skip: skipNum }, { $limit: limitNum });

                const groupedData = await TblReport2025.aggregate(pipeline).allowDiskUse(true);

                const reports = groupedData.map(item => ({
                    artist: item._id.artist || item.sampleArtist || "Unknown Artist",
                    release: item._id.release || item.sampleRelease || "Unknown Release",
                    isrc_code: item._id.isrc_code || item.sampleISRC || "Unknown",
                    territory: item._id.territory || "Global",
                    revenue: Number(item.revenue.toFixed(2)),
                    date: item.sampleDate,
                    platform: item.samplePlatform || "Various"
                }));

                return res.json({
                    success: true,
                    data: {
                        reports,
                        pagination: {
                            totalRecords,
                            totalPages: Math.ceil(totalRecords / limitNum),
                            currentPage: pageNum,
                            limit: limitNum
                        }
                    }
                });

            } else {
                // No grouping → show individual rows, paginated at DB level
                const countResult = await TblReport2025.countDocuments(filter);
                totalRecords = countResult;

                const rawData = await TblReport2025.find(filter)
                    .select('date retailer track_artist release isrc_code territory net_total percentage')
                    .skip(skipNum)
                    .limit(limitNum)
                    .lean();

                const reports = rawData.map(row => ({
                    date: row.date,
                    platform: row.retailer || "Unknown",
                    artist: row.track_artist || "Unknown Artist",
                    release: row.release || "Unknown Release",
                    isrc_code: row.isrc_code || "Unknown",
                    territory: row.territory || "Global",
                    revenue: Number((row.net_total * (100 - (row.percentage || 0)) / 100).toFixed(2))
                }));

                return res.json({
                    success: true,
                    data: {
                        reports,
                        pagination: {
                            totalRecords,
                            totalPages: Math.ceil(totalRecords / limitNum),
                            currentPage: pageNum,
                            limit: limitNum
                        }
                    }
                });
            }

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueReports:", error);
            next(error);
        }
    }

    // getYoutubeRevenueSummary method
    async getYoutubeRevenueSummary(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter ===
            const filter = {};

            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }

            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            filter.retailer = platform
                ? { $in: platform.split(",").map(p => p.trim()) }
                : { $in: defaultRetailers };

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month) {
                const start = new Date(selectedYear, month - 1, 1);
                const end = new Date(selectedYear, month, 0);
                filter.date = {
                    $gte: start.toISOString().split("T")[0],
                    $lte: end.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }


            // === Step 1: Daily aggregation ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: {
                            $convert: {
                                input: "$total_revenue",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        },
                        streamsNum: {
                            $convert: {
                                input: "$total_play",
                                to: "long",
                                onError: 0,
                                onNull: 0
                            }
                        }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" },
                        dailyStreams: { $sum: "$streamsNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        dailyStreams: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await YouTube.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Contracts & deductions ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];

            const contracts = uniqueUserIds.length
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            let totalDeductedRevenue = 0;
            let totalStreams = 0;
            let entriesWithDeduction = 0;
            let sumDeductionPercent = 0;

            dailyData.forEach(item => {
                let deducted = item.dailyRevenue;
                let percentage = 0;
                let applied = false;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        applied = true;
                        break;
                    }
                }

                if (applied) {
                    entriesWithDeduction++;
                    sumDeductionPercent += percentage;
                }

                totalDeductedRevenue += deducted;
                totalStreams += item.dailyStreams;
            });

            const avgDeductionPercentage =
                entriesWithDeduction > 0
                    ? sumDeductionPercent / entriesWithDeduction
                    : 0;

            // === Step 3: Charts ===
            const chartPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: {
                            $convert: {
                                input: "$total_revenue",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        }
                    }
                },
                {
                    $facet: {
                        byMonth: [
                            {
                                $group: {
                                    _id: {
                                        $dateToString: {
                                            format: "%b %Y",
                                            date: {
                                                $dateFromString: { dateString: "$date" }
                                            }
                                        }
                                    },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { _id: 1 } },
                            {
                                $project: {
                                    month: "$_id",
                                    revenue: { $round: ["$revenue", 2] },
                                    _id: 0
                                }
                            }
                        ],
                        byPlatform: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$retailer", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            {
                                $project: {
                                    platform: "$_id",
                                    revenue: { $round: ["$revenue", 2] },
                                    _id: 0
                                }
                            }
                        ],
                        byCountry: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$country", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 },
                            {
                                $project: {
                                    country: "$_id",
                                    revenue: { $round: ["$revenue", 2] },
                                    _id: 0
                                }
                            }
                        ]
                    }
                }
            ];

            const [chartResult] = await YouTube.aggregate(chartPipeline).allowDiskUse(true);

            const grossTotal = chartResult.byMonth.reduce((s, m) => s + m.revenue, 0);
            const deductionRatio = grossTotal > 0 ? totalDeductedRevenue / grossTotal : 1;

            const revenueByMonth = Object.fromEntries(
                chartResult.byMonth.map(m => [m.month, +(m.revenue * deductionRatio).toFixed(2)])
            );
            const revenueByChannel = Object.fromEntries(
                chartResult.byPlatform.map(p => [p.platform, +(p.revenue * deductionRatio).toFixed(2)])
            );
            const revenueByCountry = Object.fromEntries(
                chartResult.byCountry.map(c => [c.country, +(c.revenue * deductionRatio).toFixed(2)])
            );

            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams,
                        totalRevenue: +totalDeductedRevenue.toFixed(2),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: +avgDeductionPercentage.toFixed(2),
                        entriesWithDeduction,
                        totalEntries: dailyData.length
                    },
                    revenueByMonth,
                    revenueByChannel,
                    revenueByCountry
                }
            });

        } catch (error) {
            console.error("Error in getYoutubeRevenueSummary:", error);
            next(error);
        }
    }

    // getYoutubeRevenueReports method
    async getYoutubeRevenueReports(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory,
                page = 1, limit = 10
            } = req.query;

            const { role, userId } = req.user;

            const filter = {};

            /** ========== USER ROLE FILTER ========== **/
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            /** ========== PLATFORM FILTER ========== **/
            const defaultRetailers = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            filter.retailer = platform
                ? { $in: platform.split(",").map(p => p.trim()) }
                : { $in: defaultRetailers };

            /** ========== DATE FILTER ========== **/
            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month) {
                const start = new Date(selectedYear, month - 1, 1);
                const end = new Date(selectedYear, month, 0);
                filter.date = {
                    $gte: start.toISOString().split("T")[0],
                    $lte: end.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            if (territory === "true")
                filter.country = { $nin: ["", null, undefined] };

            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skipNum = (pageNum - 1) * limitNum;

            /** ========== DAILY AGGREGATION ========== **/
            const dailyData = await YouTube.aggregate([
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: {
                            $convert: {
                                input: "$total_revenue",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        _id: 0
                    }
                }
            ]).allowDiskUse(true);

            /** ========== CONTRACT DEDUCTION ========== **/
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];

            const contracts = uniqueUserIds.length
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            const dailyDeducted = dailyData.map(item => {
                let deducted = item.dailyRevenue;
                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        const percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        break;
                    }
                }
                return { date: item.date, user_id: item.user_id, deductedRevenue: deducted };
            });

            /** ========== GROUPING ========== **/
            let groupId = { user_id: "$user_id" };

            let sampleFields = {
                sampleDate: { $first: "$date" },
                samplePlatform: { $first: "$retailer" },
                sampleArtist: { $first: "$track_artist" },
                sampleRelease: { $first: "$asset_title" },
                sampleISRC: { $first: "$isrc_code" }
            };

            if (artist === "true")
                groupId.artist = { $ifNull: ["$track_artist", "Unknown Channel"] };

            if (releases === "true")
                groupId.release = { $ifNull: ["$asset_title", "Unknown Asset"] };

            if (territory === "true")
                groupId.country = { $ifNull: ["$country", "Unknown"] };

            if (track === "true")
                groupId.isrc_code = { $ifNull: ["$isrc_code", "Unknown"] };

            const pipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: {
                            $convert: {
                                input: "$total_revenue",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        }
                    }
                },
                {
                    $group: {
                        _id: groupId,
                        grossRevenue: { $sum: "$revenueNum" },
                        ...sampleFields
                    }
                }
            ];

            const artistData = await YouTube.aggregate(pipeline).allowDiskUse(true);

            /** ========== APPLY DEDUCTION RATIO ========== **/
            const includeTrack = track === "true";
            const includeRelease = releases === "true";

            const reports = artistData.map(item => {
                const userDaily = dailyDeducted.filter(d => d.user_id === item._id.user_id);

                const totalGross = userDaily.reduce(
                    (s, d) =>
                        s +
                        (dailyData.find(dd =>
                            dd.user_id === item._id.user_id && dd.date === d.date
                        )?.dailyRevenue || 0),
                    0
                );

                const totalDeducted = userDaily.reduce((s, d) => s + d.deductedRevenue, 0);

                const ratio = totalGross > 0 ? totalDeducted / totalGross : 1;

                const base = {
                    artist: item._id.artist || item.sampleArtist || "Unknown Channel",
                    territory: item._id.country || "Global",
                    revenue: +(item.grossRevenue * ratio).toFixed(2),
                    date: item.sampleDate,
                    platform: item.samplePlatform || "YouTube",
                    user_id: item._id.user_id
                };

                if (includeTrack) {
                    base.isrc_code = item._id.isrc_code || item.sampleISRC || "Unknown";
                } else if (includeRelease) {
                    base.release = item._id.release || item.sampleRelease || "Unknown Asset";
                }

                return base;
            }).sort((a, b) => b.revenue - a.revenue);

            /** ========== PAGINATION ========== **/
            const totalRecords = reports.length;
            const paginatedReports = reports.slice(skipNum, skipNum + limitNum);

            res.json({
                success: true,
                data: {
                    reports: paginatedReports,
                    pagination: {
                        totalRecords,
                        totalPages: Math.ceil(totalRecords / limitNum),
                        currentPage: pageNum,
                        limit: limitNum
                    }
                }
            });

        } catch (error) {
            console.error("Error in getYoutubeRevenueReports:", error);
            next(error);
        }
    }

    // downloadExcelReport method
    async triggerAudioStreamingExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await AudioStreamingReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "Report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new AudioStreamingReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "Report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting report generation",
                error: error.message
            });
        }
    }

    // Separate function to process the report
    async processAudioStreamingReport(reportId, filters) {
        try {
            console.log(`Processing report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Apple Music",
                "Spotify",
                "Gaana",
                "Jio Saavn",
                "Facebook",
                "Amazon",
                "TikTok"
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }


            const pipeline = [
                { $match: filter },
                { $sort: { date: -1 } },
                { $project: { __v: 0, createdAt: 0, updatedAt: 0 } }
            ];

            console.log(`Filter for report ${reportId}:`, JSON.stringify(filter, null, 2));

            const count = await TblReport2025.countDocuments(filter);
            console.log(`Total records found for report ${reportId}: ${count}`);

            if (count === 0) {
                await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found',
                });
                return;
            }

            const MAX_ROWS_PER_SHEET = 1000000;
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const randomSuffix = Math.random().toString(36).substring(2, 8);
            const filename = `Revenue_Report_${timestamp}_${randomSuffix}.xlsx`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // Create streaming workbook
            const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
                filename: absoluteFilePath,
                useStyles: true,
                useSharedStrings: true
            });

            let headers = ['S.No'];
            let headersDetermined = false;
            let currentWorksheet = null;
            let rowCountInCurrentSheet = 0;
            let totalRowsProcessed = 0;
            let sheetIndex = 1;

            const excludeFields = ["_id", "__v", "createdAt", "updatedAt"];

            async function createNewWorksheet() {
                if (currentWorksheet) {
                    currentWorksheet.commit();
                }

                currentWorksheet = workbook.addWorksheet(`Sheet ${sheetIndex}`);
                sheetIndex++;

                const columnDefs = [
                    { header: 'S.No', key: 'sno', width: 12 }
                ];

                headers.slice(1).forEach(h => {
                    columnDefs.push({
                        header: h,
                        key: h.toLowerCase().replace(/\s+/g, '_'),
                        width: Math.min(Math.max(h.length + 5, 15), 40)
                    });
                });

                currentWorksheet.columns = columnDefs;

                const headerRow = currentWorksheet.getRow(1);
                headerRow.font = { bold: true };
                headerRow.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: 'FFE0E0E0' }
                };

                rowCountInCurrentSheet = 0;
            }

            const collection = mongoose.connection.db.collection('tblreport_2025');
            const cursor = collection.aggregate(pipeline, {
                allowDiskUse: true,
                cursor: { batchSize: 1000 }
            });

            function addOverflowMessageRow(worksheet, headers, nextSheetNumber) {
                const messageRow = {
                    sno: ""
                };

                // Put message in first data column
                const firstDataKey = headers[1]
                    .toLowerCase()
                    .replace(/\s+/g, "_");

                messageRow[firstDataKey] =
                    `⚠ Data continues in Sheet ${nextSheetNumber}. Please check the next sheet.`;

                const row = worksheet.addRow(messageRow);

                row.font = { bold: true, italic: true };
                row.alignment = { vertical: "middle", horizontal: "left" };

                row.commit();
            }


            for await (const doc of cursor) {
                // Determine headers from the very first document
                if (!headersDetermined) {
                    Object.keys(doc).forEach(key => {
                        if (!excludeFields.includes(key) && key !== "date") {
                            headers.push(key);
                        }
                    });
                    headers.push("date");

                    // Now create the first worksheet with proper headers
                    await createNewWorksheet();
                    headersDetermined = true;
                }

                // If current sheet is full, create a new one
                if (rowCountInCurrentSheet >= MAX_ROWS_PER_SHEET) {

                    // Add message before closing current sheet
                    addOverflowMessageRow(
                        currentWorksheet,
                        headers,
                        sheetIndex   // next sheet number
                    );

                    // Commit and create next sheet
                    await createNewWorksheet();
                }


                // Build row data object
                const rowData = {
                    sno: totalRowsProcessed + 1
                };

                Object.keys(doc).forEach(key => {
                    if (!excludeFields.includes(key) && key !== "date") {
                        const normKey = key.toLowerCase().replace(/\s+/g, '_');
                        rowData[normKey] = doc[key] ?? "";
                    }
                });
                rowData.date = doc.date ?? "";

                // Add row and commit it immediately (critical for streaming stability)
                const row = currentWorksheet.addRow(rowData);
                row.commit();

                rowCountInCurrentSheet++;
                totalRowsProcessed++;

                if (totalRowsProcessed % 10000 === 0) {
                    console.log(`Processed ${totalRowsProcessed} rows... (Current sheet: Report Part ${sheetIndex - 1})`);
                }
            }

            // Commit the final worksheet
            if (currentWorksheet) {
                currentWorksheet.commit();
            }

            // Finalize the workbook
            await workbook.commit();

            cursor.close();

            console.log(`Excel report generated: ${absoluteFilePath}`);
            console.log(`Total rows: ${totalRowsProcessed} across ${sheetIndex - 1} sheet(s)`);

            // Update history (single file)
            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename,
                filePath: relativePath,
                fileURL
            });

            console.log(`Report ${reportId} successfully generated as Excel (.xlsx)`);

        } catch (error) {
            console.error(`Error processing report ${reportId}:`, error);
            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message || 'Unknown error',
            });
            throw error;
        }
    }

    // Trigger YouTube report generation
    async triggerYoutubeExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await YoutubeReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "YouTube report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new YoutubeReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "YouTube report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering YouTube report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting YouTube report generation",
                error: error.message
            });
        }
    }

    // Process YouTube report
    async processYoutubeReport(reportId, filters) {
        try {
            console.log(`Processing YouTube report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "SoundRecording",
                "YouTubeArtTrack",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue"
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            const pipeline = [
                { $match: filter },
                { $sort: { date: -1 } },
                { $project: { __v: 0, createdAt: 0, updatedAt: 0, uploadId: 0 } }
            ];

            console.log(`Filter for YouTube report ${reportId}:`, JSON.stringify(filter, null, 2));

            // Use the 'youtube' collection
            const collection = mongoose.connection.db.collection('youtubes');
            const count = await collection.countDocuments(filter);
            console.log(`Total records found for YouTube report ${reportId}: ${count}`);

            if (count === 0) {
                await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found',
                });
                return;
            }

            const MAX_ROWS_PER_SHEET = 1000000;
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const randomSuffix = Math.random().toString(36).substring(2, 8);
            const filename = `YouTube_Revenue_Report_${timestamp}_${randomSuffix}.xlsx`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // Create streaming workbook
            const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
                filename: absoluteFilePath,
                useStyles: true,
                useSharedStrings: true
            });

            let headers = ['S.No'];
            let headersDetermined = false;
            let currentWorksheet = null;
            let rowCountInCurrentSheet = 0;
            let totalRowsProcessed = 0;
            let sheetIndex = 1;

            const excludeFields = ["_id", "__v", "createdAt", "updatedAt", "uploadId"];

            async function createNewWorksheet() {
                if (currentWorksheet) {
                    currentWorksheet.commit();
                }

                currentWorksheet = workbook.addWorksheet(`Sheet ${sheetIndex}`);
                sheetIndex++;

                const columnDefs = [
                    { header: 'S.No', key: 'sno', width: 12 }
                ];

                headers.slice(1).forEach(h => {
                    columnDefs.push({
                        header: h,
                        key: h.toLowerCase().replace(/\s+/g, '_'),
                        width: Math.min(Math.max(h.length + 5, 15), 40)
                    });
                });

                currentWorksheet.columns = columnDefs;

                const headerRow = currentWorksheet.getRow(1);
                headerRow.font = { bold: true };
                headerRow.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: 'FFE0E0E0' }
                };

                rowCountInCurrentSheet = 0;
            }

            const cursor = collection.aggregate(pipeline, {
                allowDiskUse: true,
                cursor: { batchSize: 1000 }
            });

            for await (const doc of cursor) {
                // Determine headers from the very first document
                if (!headersDetermined) {
                    Object.keys(doc).forEach(key => {
                        if (!excludeFields.includes(key) && key !== "date") {
                            headers.push(key);
                        }
                    });
                    headers.push("date"); // Date column at the end

                    await createNewWorksheet();
                    headersDetermined = true;
                }

                // If current sheet is full, create a new one
                if (rowCountInCurrentSheet >= MAX_ROWS_PER_SHEET) {
                    await createNewWorksheet();
                }

                // Build row data object
                const rowData = {
                    sno: totalRowsProcessed + 1
                };

                Object.keys(doc).forEach(key => {
                    if (!excludeFields.includes(key) && key !== "date") {
                        const normKey = key.toLowerCase().replace(/\s+/g, '_');
                        rowData[normKey] = doc[key] ?? "";
                    }
                });
                rowData.date = doc.date ?? "";

                // Add row and commit it immediately
                const row = currentWorksheet.addRow(rowData);
                row.commit();

                rowCountInCurrentSheet++;
                totalRowsProcessed++;

                if (totalRowsProcessed % 10000 === 0) {
                    console.log(`Processed ${totalRowsProcessed} rows... (Current sheet: Sheet ${sheetIndex - 1})`);
                }
            }

            // Commit the final worksheet
            if (currentWorksheet) {
                currentWorksheet.commit();
            }

            // Finalize the workbook
            await workbook.commit();
            cursor.close();

            console.log(`YouTube Excel report generated: ${absoluteFilePath}`);
            console.log(`Total rows: ${totalRowsProcessed} across ${sheetIndex - 1} sheet(s)`);

            // Update YoutubeReportHistory
            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename,
                filePath: relativePath,
                fileURL,
            });

            console.log(`YouTube Report ${reportId} successfully generated as Excel (.xlsx)`);

        } catch (error) {
            console.error(`Error processing YouTube report ${reportId}:`, error);
            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message || 'Unknown error',
            });
            throw error;
        }
    }

    // Process pending audio streaming reports
    async processPendingReports() {
        try {
            const existingGenerate = await AudioStreamingReportHistory.exists({
                status: { $in: ['generate', 'generating'] }
            });

            if (existingGenerate) {
                console.log('Generate process already in progress. Skipping pending updates.');
                return;
            }

            const updatedReports = await AudioStreamingReportHistory.updateMany(
                { status: 'pending' },
                {
                    $set: {
                        status: 'generate',
                        filename: 'Preparing to generate...'
                    }
                }
            );

            console.log(`Updated ${updatedReports.modifiedCount} reports from pending to generate`);

            const generateReports = await AudioStreamingReportHistory.find({
                status: 'generate'
            }).sort({ generatedAt: 1 });

            console.log(`Found ${generateReports.length} pending Audio reports to process`);

            for (const report of generateReports) {
                const THIRTY_MINUTES = 30 * 60 * 1000;
                const reportAge =
                    Date.now() - new Date(report.generatedAt).getTime();

                if (reportAge > THIRTY_MINUTES) {
                    await AudioStreamingReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                const lockedReport = await AudioStreamingReportHistory.findOneAndUpdate(
                    { _id: report._id, status: 'generate' },
                    {
                        $set: {
                            status: 'generating',
                            filename: 'Generating report...',
                            processingStartedAt: new Date()
                        }
                    },
                    { new: true }
                );

                if (!lockedReport) continue;

                await this.processAudioStreamingReport(report._id, report.filters);
            }
        } catch (error) {
            console.error('Error in processPendingReports cron job:', error);
        }
    }

    // Process pending YouTube reports
    async processPendingYoutubeReports() {
        try {
            const existingGenerate = await YoutubeReportHistory.exists({
                status: { $in: ['generate', 'generating'] }
            });

            if (existingGenerate) {
                console.log('Generate process already in progress. Skipping pending updates.');
                return;
            }

            const updatedReports = await YoutubeReportHistory.updateMany(
                { status: 'pending' },
                {
                    $set: {
                        status: 'generate',
                        filename: 'Preparing to generate...'
                    }
                }
            );

            console.log(`Updated ${updatedReports.modifiedCount} youtube reports from pending to generate`);

            const generateReports = await YoutubeReportHistory.find({
                status: 'generate'
            }).sort({ generatedAt: 1 });

            console.log(`Found ${generateReports.length} pending YouTube reports to process`);

            for (const report of generateReports) {
                const THIRTY_MINUTES = 30 * 60 * 1000;
                const reportAge = Date.now() - new Date(report.generatedAt).getTime();

                if (reportAge > THIRTY_MINUTES) {
                    await YoutubeReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                const lockedReport = await YoutubeReportHistory.findOneAndUpdate(
                    { _id: report._id, status: 'generate' },
                    {
                        $set: {
                            status: 'generating',
                            filename: 'Generating report...',
                            processingStartedAt: new Date()
                        }
                    },
                    { new: true }
                );

                if (!lockedReport) continue;

                await this.processYoutubeReport(report._id, report.filters);
            }
        } catch (error) {
            console.error('Error in processPendingYoutubeReports cron job:', error);
        }
    }

    // Combined cron job to process all pending reports
    async processAllPendingReports() {
        try {
            console.log('Processing all pending reports...');

            // Process audio streaming reports
            await this.processPendingReports();

            // Process YouTube reports
            await this.processPendingYoutubeReports();

            console.log('All pending reports processed');
        } catch (error) {
            console.error("Error in processAllPendingReports:", error);
        }
    }

    // deleteRevenueByUserId method
    async deleteRevenueByUserId(req, res, next) {
        try {
            const { userId } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // Delete upload entry
            const revenueUploadResult = await RevenueUpload.findByIdAndDelete(userId);

            if (!revenueUploadResult) {
                return res.status(404).json({
                    success: false,
                    message: "RevenueUpload record not found"
                });
            }

            // Delete revenue reports
            const tempReportResult = await TempReport.deleteMany({
                uploadId: userId
            });

            return res.status(200).json({
                success: true,
                message: "Revenue data deleted successfully",
                tempReportDeletedCount: tempReportResult.deletedCount,
                revenueUploadDeleted: true
            });

        } catch (error) {
            console.error("Error deleting revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //getReportHistory method
    async getReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await AudioStreamingReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt user_id')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteReportHistory method
    async deleteReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await AudioStreamingReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await AudioStreamingReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

    //getYoutubeReportHistory method
    async getYoutubeReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await YoutubeReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteYoutubeReportHistory method
    async deleteYoutubeReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await YoutubeReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await YoutubeReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

    async insertBatch(batch) {
        if (!batch || batch.length === 0) return 0;

        try {
            const result = await TblReport2025.insertMany(batch, {
                ordered: false,
                bypassDocumentValidation: true
            });

            const inserted = result.length;
            batch.length = 0; // 🔥 FREE MEMORY
            return inserted;

        } catch (error) {
            // Partial success handling
            if (error.insertedDocs) {
                const inserted = error.insertedDocs.length;
                batch.length = 0;
                return inserted;
            }

            console.error('Batch insert error:', error);
            batch.length = 0;
            return 0;
        }
    }

    async importRevenueFromJson(req, res) {
        try {
            const { filePath, uploadId, userId } = req.body;

            if (!filePath || !uploadId) {
                return res.status(400).json({
                    success: false,
                    message: 'filePath and uploadId are required'
                });
            }

            const absolutePath = path.resolve(filePath);
            console.log('absolutePath:', absolutePath);

            if (!fs.existsSync(absolutePath)) {
                return res.status(404).json({
                    success: false,
                    message: 'JSON file not found'
                });
            }

            // 🔄 Streaming pipeline (NO readFileSync)
            const pipeline = chain([
                fs.createReadStream(absolutePath, { highWaterMark: 1024 * 1024 }),
                parser(),
                streamArray()
            ]);

            let batch = [];
            let totalInserted = 0;

            for await (const { value } of pipeline) {
                const formattedDate = getDateFromMonthYear(
                    value.month,
                    value.year
                );

                batch.push({
                    user_id: value.label_id || 0,
                    uploadId,

                    // retailer: value.channel_name,
                    retailer: 'Facebook',
                    label: value.label_name || null,

                    upc_code: null,
                    catalogue_number: null,

                    isrc_code: value.isrc || value.elected_isrc || null,

                    release: value.track_name || null,
                    track_title: value.track_name || null,
                    track_artist: value.artist_name || null,
                    // release: value.asset_title || null,
                    // track_title: value.asset_title || null,
                    // track_artist: value.label_name || null,

                    remixer_name: null,
                    remix: null,

                    territory: value.country === 'N/A' ? null : value.country,

                    purchase_status: null,
                    format: value.product || null,
                    delivery: 'Streaming',

                    content_type: null,
                    track_count: value.total_play || null,
                    sale_type: null,

                    net_total: value.total_revenue || null,

                    date: formattedDate,
                    uploading_date: formattedDate
                });

                // 💾 Batch insert
                if (batch.length === BATCH_SIZE) {
                    totalInserted += await this.insertBatch(batch);
                }
            }

            // 🧹 Insert remaining
            totalInserted += await this.insertBatch(batch);
            console.log("totalInserted", totalInserted);


            return res.status(200).json({
                success: true,
                message: 'Revenue data imported successfully',
                totalInserted
            });

        } catch (error) {
            console.error('Import JSON Error:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }

    async insertYoutubeBatch(batch) {
        if (!batch || batch.length === 0) return 0;

        try {
            const result = await YouTube.insertMany(batch, {
                ordered: false,
                bypassDocumentValidation: true
            });

            const inserted = result.length;
            batch.length = 0; // 🔥 FREE MEMORY
            return inserted;

        } catch (error) {
            // Partial success handling
            if (error.insertedDocs) {
                const inserted = error.insertedDocs.length;
                batch.length = 0;
                return inserted;
            }

            console.error('Batch insert error:', error);
            batch.length = 0;
            return 0;
        }
    }


    async importYoutubeRevenueFromJson(req, res) {
        try {
            const { filePath, uploadId, userId } = req.body;

            if (!filePath || !uploadId) {
                return res.status(400).json({
                    success: false,
                    message: "filePath and uploadId are required"
                });
            }

            const absolutePath = path.resolve(filePath);
            console.log("absolutePath:", absolutePath);

            if (!fs.existsSync(absolutePath)) {
                return res.status(404).json({
                    success: false,
                    message: "JSON file not found"
                });
            }

            // Streaming instead of readFileSync
            const pipeline = chain([
                fs.createReadStream(absolutePath, { highWaterMark: 1024 * 1024 }),
                parser(),
                streamArray()
            ]);

            let batch = [];
            let totalInserted = 0;

            for await (const { value } of pipeline) {
                const formattedDate = getDateFromMonthYear(value.month, value.year);

                batch.push({
                    user_id: value.label_id || null,
                    uploadId,
                    type: value.type || null,
                    asset_id: value.asset_id || null,
                    country: value.country === "N/A" ? null : value.country,
                    isrc_code: value.isrc || value.elected_isrc || null,
                    upc_code: value.upc || null,
                    sub_label_id: value.sub_label_id || null,
                    sub_label_share: value.sub_label_share || null,
                    partner_share: value.partner_share || null,
                    content_type: value.content_type || null,
                    claim_type: value.claim_type || null,
                    asset_title: value.asset_title || null,
                    video_duration_sec: value.video_duration_sec || null,
                    category: value.category || null,
                    custom_id: value.custom_id || null,
                    asset_channel_id: value.asset_channel_id || null,
                    channel_name: value.channel_name || null,
                    label_name: value.label_name || null,
                    total_play: value.total_play ? Number(value.total_play) : null,
                    partner_revenue: value.partner_revenue || null,
                    inr_rate: value.inr_rate || null,
                    total_revenue: value.total_revenue || null,
                    label_shared: value.label_shared || null,
                    track_id: value.track_id || null,
                    album_id: value.album_id || null,
                    channel_type: value.channel_type || 1,
                    usd: value.usd || null,
                    usd_label_share: value.usd_label_share || null,
                    usd_rdc_share: value.usd_rdc_share || null,
                    label_share: value.label_share || null,
                    rdc_share: value.rdc_share || null,
                    fileid: value.fileid || null,
                    status: 0,
                    inv_generated: false,
                    label_code: value.label_code || null,
                    video_link: value.video_link || null,
                    channel_link: value.channel_link || null,
                    sub_label: value.sub_label || null,
                    date: formattedDate,
                    uploading_date: new Date().toISOString().split("T")[0]
                });

                if (batch.length === BATCH_SIZE) {
                    totalInserted += await this.insertYoutubeBatch(batch);
                }
            }

            // Insert Remaining
            totalInserted += await this.insertYoutubeBatch(batch);

            return res.status(200).json({
                success: true,
                message: "YouTube revenue imported successfully",
                totalInserted
            });

        } catch (error) {
            console.error("Import JSON Error:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //calculateRevenueSummary method
    async calculateRevenueSummary(userId) {
        try {
            // Fetch the user's role to decide if we calculate global or per-user
            const user = await User.findOne({ id: userId }).select('role').lean();
            console.log("user", user);

            if (!user) {
                console.error(`User not found for id: ${userId}`);
                return;
            }

            const isAdmin = ['Super Admin', 'Manager'].includes(user.role);

            const now = new Date();
            const startDate = new Date(now.getFullYear(), now.getMonth() - 11, 1);
            const endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);

            const dateFilter = {
                $gte: startDate.toISOString().split("T")[0],
                $lte: endDate.toISOString().split("T")[0]
            };

            // Build match stage — remove user_id filter if admin
            const matchStage = isAdmin
                ? { date: dateFilter }
                : { user_id: userId, date: dateFilter };

            const dailyPipeline = [
                { $match: isAdmin ? {} : { user_id: userId } },
                {
                    $addFields: {
                        // Fix: Extract only numbers and decimal point, limit to 2 decimal places
                        revenueNum: {
                            $convert: {
                                input: "$net_total",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        }
                        ,
                        streamsNum: {
                            $convert: {
                                input: "$track_count",
                                to: "long",
                                onError: 0,
                                onNull: 0
                            }
                        }

                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        revenue: { $sum: "$revenueNum" },
                        streams: { $sum: "$streamsNum" }
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // Rest of the function remains the same...
            const uniqueUserIds = [...new Set(dailyData.map(d => d._id.user_id))];

            const contracts = uniqueUserIds.length
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            let totalRevenue = 0;
            let totalStreams = 0;

            dailyData.forEach(item => {
                let revenue = item.revenue;

                const userContracts = contractMap.get(item._id.user_id) || [];
                for (const c of userContracts) {
                    if (item._id.date >= c.startDate && item._id.date <= c.endDate) {
                        revenue *= (100 - (c.labelPercentage || 0)) / 100;
                        break;
                    }
                }

                totalRevenue += revenue;
                totalStreams += item.streams;
            });

            console.log("totalRevenue", totalRevenue);
            console.log("totalStreams", totalStreams);

            const chartPipeline = [
                { $match: matchStage },
                {
                    $addFields: {
                        // Apply the same cleaning here
                        revenueNum: {
                            $cond: [
                                { $in: [{ $type: "$net_total" }, ["double", "int", "long", "decimal"]] },
                                "$net_total",
                                {
                                    $cond: [
                                        { $eq: [{ $type: "$net_total" }, "string"] },
                                        { $toDouble: "$net_total" },
                                        0
                                    ]
                                }
                            ]
                        }

                    }
                },
                {
                    $facet: {
                        byMonth: [
                            {
                                $group: {
                                    _id: {
                                        $dateToString: {
                                            format: "%Y-%m",
                                            date: { $dateFromString: { dateString: "$date" } }
                                        }
                                    },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { _id: 1 } }
                        ],
                        byChannel: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$retailer", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            }
                        ],
                        byCountry: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$territory", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 }
                        ]
                    }
                }
            ];

            const [chartData] = await TblReport2025.aggregate(chartPipeline).allowDiskUse(true);

            const last12Months = getLast12Months();
            const grossTotal = chartData.byMonth.reduce((s, m) => s + m.revenue, 0);
            const ratio = grossTotal > 0 ? totalRevenue / grossTotal : 1;

            const monthMap = Object.fromEntries(chartData.byMonth.map(m => [m._id, m.revenue]));

            const netRevenueByMonth = {};
            last12Months.forEach(m => {
                netRevenueByMonth[m] = Number(((monthMap[m] || 0) * ratio).toFixed(2));
            });

            const revenueByChannel = Object.fromEntries(
                chartData.byChannel.map(c => [c._id || "Unknown", Number((c.revenue * ratio).toFixed(2))])
            );

            const revenueByCountry = Object.fromEntries(
                chartData.byCountry.map(c => [c._id || "Unknown", Number((c.revenue * ratio).toFixed(2))])
            );

            // Decide where to save: per user or global
            const saveUserId = isAdmin ? 'global' : userId;

            await RevenueSummary.updateOne(
                { user_id: saveUserId },
                {
                    $set: {
                        netRevenueByMonth,
                        revenueByChannel,
                        revenueByCountry
                    },
                    $setOnInsert: { user_id: saveUserId }
                },
                { upsert: true }
            );

            // Only update User totals if it's a regular user (not admin/global)
            if (!isAdmin) {
                await User.updateOne(
                    { id: userId },
                    {
                        $set: {
                            total_stream: totalStreams,
                            total_revenue: Number(totalRevenue.toFixed(2))
                        }
                    }
                );
            }

        } catch (error) {
            console.log(error);
        }
    }

    //calculateRevenueForSuperAdminandManager method
    async calculateRevenueForSuperAdminandManager() {
        try {
            // Fetch all Super Admin and Manager users
            const admins = await User.find({ role: { $in: ['Super Admin', 'Manager'] } }).lean();
            if (!admins.length) {
                console.log("No Super Admin or Manager found");
                return;
            }

            const now = new Date();
            const startDate = new Date(now.getFullYear(), now.getMonth() - 11, 1);
            const endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);

            const dateFilter = {
                $gte: startDate.toISOString().split("T")[0],
                $lte: endDate.toISOString().split("T")[0]
            };

            const dailyPipeline = [
                {
                    $addFields: {
                        // Handle empty/null values properly
                        revenueNum: {
                            $convert: {
                                input: "$net_total",
                                to: "double",
                                onError: 0,
                                onNull: 0
                            }
                        },
                        streamsNum: {
                            $convert: {
                                input: "$track_count",
                                to: "long",
                                onError: 0,
                                onNull: 0
                            }
                        }

                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        revenue: { $sum: "$revenueNum" },
                        streams: { $sum: "$streamsNum" }
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            const uniqueUserIds = [...new Set(dailyData.map(d => d._id.user_id))];

            const contracts = uniqueUserIds.length
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            let totalRevenue = 0;
            let totalStreams = 0;

            dailyData.forEach(item => {
                let revenue = item.revenue;

                const userContracts = contractMap.get(item._id.user_id) || [];
                for (const c of userContracts) {
                    if (item._id.date >= c.startDate && item._id.date <= c.endDate) {
                        revenue *= (100 - (c.labelPercentage || 0)) / 100;
                        break;
                    }
                }

                totalRevenue += revenue;
                totalStreams += item.streams;
            });
            console.log("totalRevenue", totalRevenue);
            console.log("totalStreams", totalStreams);

            const chartPipeline = [
                { $match: { date: dateFilter } },
                {
                    $addFields: {
                        // Apply the same cleaning here
                        revenueNum: {
                            $cond: [
                                { $in: [{ $type: "$net_total" }, ["double", "int", "long", "decimal"]] },
                                "$net_total",
                                {
                                    $cond: [
                                        { $eq: [{ $type: "$net_total" }, "string"] },
                                        { $toDouble: "$net_total" },
                                        0
                                    ]
                                }
                            ]
                        }

                    }
                },
                {
                    $facet: {
                        byMonth: [
                            {
                                $group: {
                                    _id: {
                                        $dateToString: {
                                            format: "%Y-%m",
                                            date: { $dateFromString: { dateString: "$date" } }
                                        }
                                    },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { _id: 1 } }
                        ],
                        byChannel: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$retailer", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            }
                        ],
                        byCountry: [
                            {
                                $group: {
                                    _id: { $ifNull: ["$territory", "Unknown"] },
                                    revenue: { $sum: "$revenueNum" }
                                }
                            },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 }
                        ]
                    }
                }
            ];

            const [chartData] = await TblReport2025.aggregate(chartPipeline).allowDiskUse(true);

            const last12Months = getLast12Months();
            const grossTotal = chartData.byMonth.reduce((s, m) => s + m.revenue, 0);
            const ratio = grossTotal > 0 ? totalRevenue / grossTotal : 1;

            const monthMap = Object.fromEntries(chartData.byMonth.map(m => [m._id, m.revenue]));

            const netRevenueByMonth = {};
            last12Months.forEach(m => {
                netRevenueByMonth[m] = Number(((monthMap[m] || 0) * ratio).toFixed(2));
            });

            const revenueByChannel = Object.fromEntries(
                chartData.byChannel.map(c => [c._id || "Unknown", Number((c.revenue * ratio).toFixed(2))])
            );

            const revenueByCountry = Object.fromEntries(
                chartData.byCountry.map(c => [c._id || "Unknown", Number((c.revenue * ratio).toFixed(2))])
            );

            // Update for each Super Admin and Manager
            for (const admin of admins) {
                const adminUserId = admin.id; // Assuming 'id' is the field for user_id

                await RevenueSummary.updateOne(
                    { user_id: adminUserId },
                    {
                        $set: {
                            netRevenueByMonth,
                            revenueByChannel,
                            revenueByCountry
                        },
                        $setOnInsert: { user_id: adminUserId }
                    },
                    { upsert: true }
                );

                await User.updateOne(
                    { id: adminUserId },
                    {
                        $set: {
                            total_stream: totalStreams,
                            total_revenue: Number(totalRevenue.toFixed(2))
                        }
                    }
                );
            }

        } catch (error) {
            console.log(error);
        }
    }

    //getUserRevenueSummary method
    async getUserRevenueSummary(req, res, next) {
        try {
            const { userId } = req.user;

            const data = await User.aggregate([
                {
                    $match: { id: userId }
                },
                {
                    $lookup: {
                        from: "revenuesummaries",
                        localField: "id",
                        foreignField: "user_id",
                        as: "revenueSummary"
                    }
                },
                {
                    $unwind: {
                        path: "$revenueSummary",
                        preserveNullAndEmptyArrays: true
                    }
                },
                {
                    $project: {
                        _id: 0,
                        id: 1,
                        name: 1,
                        email: 1,
                        total_stream: 1,
                        total_revenue: 1,
                        netRevenueByMonth: "$revenueSummary.netRevenueByMonth",
                        revenueByChannel: "$revenueSummary.revenueByChannel",
                        revenueByCountry: "$revenueSummary.revenueByCountry",
                        updatedAt: "$revenueSummary.updatedAt"
                    }
                }
            ]);

            return res.json({
                success: true,
                data: data[0] || null
            });

        } catch (error) {
            next(error);
        }
    }


}

module.exports = new revenueUploadController();