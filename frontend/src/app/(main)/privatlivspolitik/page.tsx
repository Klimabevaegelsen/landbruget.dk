import { Container } from "@/components/layout/container";
import { Metadata } from "next";
import fs from "fs";
import path from "path";
import { remark } from "remark";
import html from "remark-html";

export const metadata: Metadata = {
  title: "Privatlivspolitik - Landbruget.dk",
  description: "Læs vores privatlivspolitik for Landbruget.dk - hvordan vi behandler personoplysninger i henhold til GDPR.",
};

async function getPrivacyContent() {
  const privacyPath = path.join(process.cwd(), "src", "content", "privacy-policy.md");
  const fileContents = fs.readFileSync(privacyPath, "utf8");

  const processedContent = await remark().use(html).process(fileContents);
  return processedContent.toString();
}

export default async function PrivacyPage() {
  const content = await getPrivacyContent();

  return (
    <div className="bg-white">
      <Container className="py-16 lg:py-24">
        <div className="mx-auto max-w-4xl">
          <div
            className="max-w-none
              [&_h1]:text-5xl [&_h1]:font-black [&_h1]:text-primary [&_h1]:mb-12 [&_h1]:leading-tight [&_h1]:tracking-tight
              [&_h2]:text-3xl [&_h2]:font-bold [&_h2]:text-primary [&_h2]:mt-20 [&_h2]:mb-10 [&_h2]:leading-tight [&_h2]:tracking-tight [&_h2]:border-b [&_h2]:border-primary/20 [&_h2]:pb-4
              [&_h3]:text-2xl [&_h3]:font-bold [&_h3]:text-primary-darker [&_h3]:mt-16 [&_h3]:mb-8 [&_h3]:leading-tight [&_h3]:tracking-tight
              [&_p]:text-gray-700 [&_p]:text-lg [&_p]:leading-[1.8] [&_p]:mb-8 [&_p]:font-normal
              [&_strong]:text-gray-900 [&_strong]:font-bold
              [&_em]:text-gray-600 [&_em]:italic
              [&_ul]:my-10 [&_ul]:list-disc [&_ul]:list-outside [&_ul]:space-y-4 [&_ul]:ml-6
              [&_li]:text-lg [&_li]:leading-[1.8] [&_li]:text-gray-700 [&_li]:pl-2 [&_li]:marker:text-primary
              [&_ol]:my-10 [&_ol]:list-decimal [&_ol]:list-inside [&_ol]:space-y-4
              [&_hr]:border-primary/20 [&_hr]:my-12 [&_hr]:border-t"
            dangerouslySetInnerHTML={{ __html: content }}
          />
        </div>
      </Container>
    </div>
  );
}
