
function contractReminderTemplate(userName, contractName, endDate) {
    return `
        <div style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: #4B0082;">Contract Reminder</h2>
            <p>Dear <strong>${userName}</strong>,</p>
            <p>This is a friendly reminder that your contract <strong>"${contractName}"</strong> is approaching its end date.</p>
            <p><strong>End Date:</strong> ${new Date(endDate).toLocaleDateString()}</p>
            <p>Please contact our team if you wish to renew or extend your contract.</p>
            <br/>
            <p>Best regards,</p>
            <p><strong>RDC System</strong></p>
        </div>
    `;
}

module.exports = contractReminderTemplate;
