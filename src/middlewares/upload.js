const multer = require("multer");
const path = require("path");
const fs = require("fs");

// Create folder if not exists
function ensureFolder(dir) {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
}

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        let uploadPath;

        // Excel → upload in /uploads/revenue
        if (
            file.mimetype === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
            file.mimetype === "application/vnd.ms-excel" ||
            file.mimetype === "text/csv"
        ) {
            uploadPath = path.join(__dirname, "../uploads/revenues");
        }

        // PDF → upload in /uploads/contracts
        else if (file.mimetype === "application/pdf") {
            uploadPath = path.join(__dirname, "../uploads/contracts");
        }

        else {
            return cb(new Error("Only PDF, XLS, XLSX, CSV files allowed"));
        }

        ensureFolder(uploadPath);
        cb(null, uploadPath);
    },

    filename: (req, file, cb) => {
        const fileName = Date.now() + "-" + file.originalname;
        cb(null, fileName);
    }
});

const upload = multer({ storage });

module.exports = upload;
