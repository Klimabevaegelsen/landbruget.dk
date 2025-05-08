import { NavBanner } from "@/components/layout/templates/nav-banner";
import { Navbar } from "@/components/layout/templates/navbar";
import { Footer } from "@/components/layout/templates/footer";
export default function MainLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="overflow-hidden relative">
      <Navbar banner={<NavBanner />} />
      <main>{children}</main>
      <Footer />
    </div>
  );
}
