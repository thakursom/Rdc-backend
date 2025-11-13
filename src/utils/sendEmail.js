const nodemailer = require("nodemailer");

const sendEmail = async (to, subject, html) => {
  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS
    }
  });

  const mailOptions = {
    from: `"RDC System" <${process.env.SMTP_USER}>`,
    to,
    subject,
    html
  };

  return transporter.sendMail(mailOptions);
};

module.exports = sendEmail;
