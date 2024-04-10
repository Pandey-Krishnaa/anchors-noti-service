import nodemailer from "nodemailer";

const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 465,
  secure: true, // Use `true` for port 465, `false` for all other ports
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_USER_PASS,
  },
});

export const sendEmail = async (data) => {
  try {
    await transporter.sendMail({
      from: process.env.SMTP_USER,
      to: data.to,
      subject: data.subject,
      html: data.html,
      text: data.text,
    });
  } catch (error) {
    throw error;
  }
};
