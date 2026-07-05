import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "APE Dashboard",
  description: "Read-only observer dashboard scaffold for APE."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
