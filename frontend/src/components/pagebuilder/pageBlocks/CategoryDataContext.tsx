'use client';

import React, { createContext, useContext, ReactNode } from 'react';

interface CategoryDataContextType {
  isInCategoryWithData: boolean;
}

const CategoryDataContext = createContext<CategoryDataContextType>({
  isInCategoryWithData: false,
});

export function useCategoryDataContext() {
  return useContext(CategoryDataContext);
}

interface CategoryDataProviderProps {
  children: ReactNode;
  hasData: boolean;
}

export function CategoryDataProvider({
  children,
  hasData,
}: CategoryDataProviderProps) {
  return (
    <CategoryDataContext.Provider value={{ isInCategoryWithData: hasData }}>
      {children}
    </CategoryDataContext.Provider>
  );
}
