const ResponseService = require("../services/responseService");

const User = require("../models/userModel");
const Artist = require("../models/artistModel");
const TblReport2025 = require("../models/tblReport2025Model");

class ArtistController {

    constructor() { }

    //getAllArtists method
    async getAllArtists(req, res, next) {
        try {
            const { role, userId } = req.user;
            const { page = 1, limit = 20, search } = req.query;

            const skip = (page - 1) * limit;

            let query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.created_by = userId;
            }

            if (search && search.trim() !== "") {
                query.name = new RegExp(search, "i");
            }

            const artists = await Artist.find(query)
                .skip(skip)
                .limit(Number(limit))
                .lean();

            const total = await Artist.countDocuments(query);
            const stats = await TblReport2025.aggregate([
                {
                    $group: {
                        _id: "$track_artist",
                        totalStream: {
                            $sum: {
                                $convert: {
                                    input: "$track_count",
                                    to: "int",
                                    onError: 0,
                                    onNull: 0
                                }
                            }
                        },
                        totalRevenue: {
                            $sum: {
                                $convert: {
                                    input: "$net_total",
                                    to: "double",
                                    onError: 0,
                                    onNull: 0
                                }
                            }
                        },
                        countries: { $addToSet: "$territory" }
                    }
                }
            ]);

            // Map for fast lookup
            let statsMap = {};
            stats.forEach(s => (statsMap[s._id] = s));

            artists.forEach(a => {
                const s = statsMap[a.name];
                a.totalStream = s?.totalStream || 0;
                a.totalRevenue = s?.totalRevenue || 0;
                a.countries = s?.countries || [];
            });

            return ResponseService.success(res, "Artists fetched successfully....", {
                artists,
                pagination: {
                    total,
                    page: Number(page),
                    limit: Number(limit),
                    totalPages: Math.ceil(total / limit)
                }
            });

        } catch (error) {
            return ResponseService.error(res, "Failed to fetch artists", 500, error);
        }
    }

    //fetchArtistById method
    async fetchArtistById(req, res) {
        try {
            const { id } = req.query;

            if (!id) {
                return ResponseService.error(res, "id is required", 400);
            }

            const artist = await Artist.find({ id: id });

            if (!artist) {
                return ResponseService.error(res, "Artist not found", 404);
            }

            return ResponseService.success(res, "Artist details fetched successfully", { artist });

        } catch (error) {
            return ResponseService.error(res, "Failed to fetch artist", 500, error);
        }
    }

    //fetchUserAndSubUsersArtist method
    async fetchUserAndSubUsersArtist(req, res) {
        try {
            const { id } = req.query;

            if (!id) {
                return ResponseService.error(res, "id is required", 400);
            }

            const numericId = Number(id);

            // Find main user + sub users
            const users = await User.find({
                $or: [
                    { id: numericId },
                    { parent_id: numericId }
                ]
            });

            // Extract all user ids
            const userIds = users.map(u => u.id);

            // Fetch all artists created by these users
            const artists = await Artist.find({
                created_by: { $in: userIds }
            });

            return ResponseService.success(res, "Artist details fetched successfully", { artists });

        } catch (error) {
            return res.status(500).json({ success: false, message: error.message });
        }
    }

    //fetchArtistByName method
    async fetchArtistByName(req, res) {
        try {
            let { artistName, page = 1, limit = 10, search, fromDate, toDate } = req.query;

            if (!artistName) {
                return res.status(400).json({
                    success: false,
                    message: "artistName is required",
                });
            }

            page = Number(page);
            limit = Number(limit);
            const skip = (page - 1) * limit;

            // Base match for topRelease/topDSP/topCountry (ignore filters)
            const baseMatch = {
                track_artist: { $regex: artistName, $options: "i" },
            };

            // Match for filtered records (apply search/date filters)
            const recordMatch = { ...baseMatch };
            if (search && search.trim() !== "") {
                recordMatch.release = new RegExp(search, "i");
            }
            if (fromDate || toDate) {
                recordMatch.date = {};
                if (fromDate) recordMatch.date.$gte = fromDate;
                if (toDate) recordMatch.date.$lte = toDate;
            }

            const result = await TblReport2025.aggregate([
                {
                    $facet: {
                        // Filtered records
                        records: [
                            { $match: recordMatch },
                            { $sort: { date: -1 } },
                            { $skip: skip },
                            { $limit: limit }
                        ],

                        // Total count for filtered records
                        totalCount: [
                            { $match: recordMatch },
                            { $count: "count" },
                        ],

                        // Top Release (unfiltered except artistName)
                        topRelease: [
                            { $match: baseMatch },
                            {
                                $group: {
                                    _id: "$release",
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 }
                        ],

                        // Top DSP (unfiltered except artistName)
                        topDSP: [
                            { $match: baseMatch },
                            {
                                $group: {
                                    _id: "$retailer",
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 }
                        ],

                        // Top Country (unfiltered except artistName)
                        topCountry: [
                            { $match: baseMatch },
                            {
                                $group: {
                                    _id: "$territory",
                                    totalRevenue: { $sum: { $toDouble: "$net_total" } }
                                }
                            },
                            { $sort: { totalRevenue: -1 } },
                            { $limit: 1 }
                        ]
                    }
                }
            ]);

            const response = result[0];

            return res.json({
                success: true,
                pagination: {
                    page,
                    limit,
                    totalCount: response.totalCount[0]?.count || 0,
                    totalPages: Math.ceil((response.totalCount[0]?.count || 0) / limit),
                },
                data: response.records,
                topRelease: response.topRelease[0] || null,
                topDSP: response.topDSP[0] || null,
                topCountry: response.topCountry[0] || null,
            });

        } catch (error) {
            console.log("fetchArtistByName Error:", error);
            return res.status(500).json({
                success: false,
                message: error.message,
            });
        }
    }

}

module.exports = new ArtistController();
