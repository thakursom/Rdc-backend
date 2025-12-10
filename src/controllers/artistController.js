const Artist = require("../models/artistModel");
const ResponseService = require("../services/responseService");
const User = require("../models/userModel");
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

            // SAFE AGGREGATION
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


}

module.exports = new ArtistController();
