import { create } from "zustand";

interface HashState {
  currentHash: string;
  setCurrentHash: (hash: string) => void;
}

export const useHashStore = create<HashState>((set) => ({
  currentHash: "",
  setCurrentHash: (hash: string) => set({ currentHash: hash }),
}));
