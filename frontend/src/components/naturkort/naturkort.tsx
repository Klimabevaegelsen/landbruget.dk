"use client";

import {useState} from "react";
import {NaturKort, CriteriaSelector} from "./naturkort_elements";

export function NaturKortPage() {

    const [criteriaValues, setCriteriaValues] = useState({
        biodiversitet: 1.0,
        klima: 1.0,
        nitrogen: 1.0,
        rekreation: 1.0,
      });


    function handleCriteriaChange(name, newValue) {
        console.log(name, newValue)
        setCriteriaValues(prev => ({
          ...prev,
          [name]: newValue,
        }));
    }

      return (
        <div className="flex">
            <div className="w-1/4">
                <CriteriaSelector
                    criteriaValues={criteriaValues}
                    handleCriteriaChange={handleCriteriaChange}
                />
            </div>
            <div className="w-3/4">
                <NaturKort criteriaValues={criteriaValues} />
            </div>
        </div>
      );

}