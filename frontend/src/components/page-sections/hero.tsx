import { GlobalSearch } from "../global-search";

export default function Hero() {
  return (
    <div className="relative isolate px-6 pt-14 lg:px-8">
      <div className="mx-auto max-w-4xl py-18 sm:py-28 lg:py-40">
        <div className="text-center flex flex-col gap-6">
          <h1 className="text-5xl font-bold tracking-tight text-balance text-white sm:text-5xl">
            Dansk landbrugsdata - samlet ét sted
          </h1>
          <div className="md:mx-auto">
            <div className="w-full md:w-[500px]">
              <GlobalSearch
                className=""
                borderless
                searchSuggestions={["Arla foods", "Fyn", "John Andersen"]}
              />
            </div>
          </div>

          <p className="text-sm font-medium text-pretty text-white sm:text-xl/8">
            <span className="font-bold"> 123.300 datapunkter</span> fordelt på
            <span className="font-bold">
              2.643 danske landbrugsvirksomheder
            </span>
            .<br /> Data gennemsigtighed. Fri adgang og open source.
          </p>
        </div>
      </div>
    </div>
  );
}
